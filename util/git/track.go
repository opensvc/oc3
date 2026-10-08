package git

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

// Track keeps the history of one text document per object in git, as the
// historical collector's gittrack module does for forms: one repository per
// object id under Dir, holding the document in a file named File.
type Track struct {
	Dir  string
	File string
}

// revPattern bounds the revisions passed to git: a commit id, a ref and the
// usual suffixes, never an option.
var revPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/^~@{}-]*$`)

// ValidRev reports whether s may be passed to git as a revision.
func ValidRev(s string) bool { return revPattern.MatchString(s) }

func (t Track) repo(id string) string { return filepath.Join(t.Dir, id) }

func (t Track) git(id string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"--git-dir=" + filepath.Join(t.repo(id), ".git")}, args...)...)
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(errOut.String()))
	}
	return out.String(), nil
}

func (t Track) gitIn(id string, args ...string) error {
	cmd := exec.Command("git", args...)
	cmd.Dir = t.repo(id)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Exists reports whether the object has a history.
func (t Track) Exists(id string) bool {
	_, err := os.Stat(filepath.Join(t.repo(id), ".git"))
	return err == nil
}

// authorArg formats "Name <email>" for git --author, which requires the angle
// brackets even without an address.
func authorArg(author string) []string {
	if author == "" {
		return nil
	}
	if !strings.Contains(author, "<") {
		author += " <>"
	}
	return []string{"--author=" + author}
}

func (t Track) init(id, author string) error {
	if err := os.MkdirAll(t.repo(id), 0o755); err != nil {
		return err
	}
	email := viper.GetString("git.user_email")
	if email == "" {
		email = "nobody@localhost.localdomain"
	}
	for _, args := range [][]string{
		{"init"},
		{"config", "user.email", email},
		{"config", "user.name", "collector"},
	} {
		if err := t.gitIn(id, args...); err != nil {
			return err
		}
	}
	return nil
}

// Commit records a new content of the object's document.
func (t Track) Commit(id, content, author string) error {
	_, _, err := t.CommitMessage(id, content, author, "change")
	return err
}

// CommitMessage records a new content of the object's document with a message,
// and returns the id of the commit, or changed false when the content is the
// one recorded last: nothing to commit then, which is not an error.
func (t Track) CommitMessage(id, content, author, message string) (commit string, changed bool, err error) {
	if !t.Exists(id) {
		if err := t.init(id, author); err != nil {
			return "", false, err
		}
	}
	_ = os.Remove(filepath.Join(t.repo(id), ".git", "index.lock"))
	if err := os.WriteFile(filepath.Join(t.repo(id), t.File), []byte(content), 0o644); err != nil {
		return "", false, err
	}
	if err := t.gitIn(id, "add", t.File); err != nil {
		return "", false, err
	}
	// Nothing staged against the last commit: the content did not change.
	if _, err := t.git(id, "rev-parse", "--verify", "HEAD"); err == nil {
		if err := t.gitIn(id, "diff", "--cached", "--quiet"); err == nil {
			head, err := t.git(id, "rev-parse", "HEAD")
			return strings.TrimSpace(head), false, err
		}
	}
	args := append([]string{"commit", "-m", message}, authorArg(author)...)
	if err := t.gitIn(id, args...); err != nil {
		return "", false, err
	}
	head, err := t.git(id, "rev-parse", "HEAD")
	return strings.TrimSpace(head), true, err
}

// Read returns the current content of the object's document.
func (t Track) Read(id string) (string, error) {
	b, err := os.ReadFile(filepath.Join(t.repo(id), t.File))
	return string(b), err
}

// Revision is one entry of an object's history, as gittrack's parse_log()
// returns them for objects other than sysreports.
type Revision struct {
	ID      string   `json:"id"`
	CID     string   `json:"cid"`
	Start   string   `json:"start"`
	Content string   `json:"content,omitempty"`
	Stat    []string `json:"stat"`
	Summary string   `json:"summary,omitempty"`
	Group   string   `json:"group"`
}

// Timeline returns the last 300 revisions of the object, newest first.
func (t Track) Timeline(id string) ([]Revision, error) {
	if !t.Exists(id) {
		return []Revision{}, nil
	}
	out, err := t.git(id, "log", "-n", "300", "--stat=510,500", "--date=iso", "--all")
	if err != nil {
		return nil, err
	}
	return parseLog(out, id), nil
}

func parseLog(s, id string) []Revision {
	var data []Revision
	var d Revision
	var stat strings.Builder
	flush := func() {
		if d.Start == "" {
			return
		}
		changed := map[string]bool{}
		for _, line := range strings.Split(stat.String(), "\n") {
			if strings.Contains(line, "files changed") {
				d.Summary = strings.TrimSpace(line)
				continue
			}
			if !strings.Contains(line, " | ") {
				continue
			}
			fpath := strings.Trim(strings.TrimSpace(strings.SplitN(line, " | ", 2)[0]), `"`)
			changed[fpath] = true
		}
		d.Stat = make([]string, 0, len(changed))
		for fpath := range changed {
			d.Stat = append(d.Stat, fpath)
		}
		sort.Strings(d.Stat)
		d.Group = id
		data = append(data, d)
	}
	for _, line := range strings.Split(s, "\n") {
		switch {
		case strings.HasPrefix(line, "commit"):
			flush()
			d = Revision{}
			stat.Reset()
			if fields := strings.Fields(line); len(fields) > 1 {
				d.CID = fields[1]
				d.ID = fields[1]
			}
		case strings.HasPrefix(line, "Author:"):
			d.Content = line
		case strings.HasPrefix(strings.TrimSpace(line), "rollback"):
			d.Content += "<br>" + line
		case strings.HasPrefix(line, "Date:"):
			if fields := strings.Fields(line); len(fields) >= 3 {
				d.Start = fields[1] + "T" + fields[2]
			}
		case d.CID != "" && d.Start != "":
			stat.WriteString(line + "\n")
		}
	}
	flush()
	if data == nil {
		data = []Revision{}
	}
	return data
}

// Blob is the content of the document at a revision.
type Blob struct {
	OID     string `json:"oid"`
	Content string `json:"content"`
}

// At returns the document at a revision, nil when the revision holds none.
func (t Track) At(id, rev string) (*Blob, error) {
	if !t.Exists(id) {
		return nil, nil
	}
	out, err := t.git(id, "ls-tree", "-r", rev)
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		content, err := t.git(id, "show", fields[2])
		if err != nil {
			return nil, err
		}
		return &Blob{OID: fields[2], Content: content}, nil
	}
	return nil, nil
}

// Show returns the change made by a revision, with its stats, as git prints it.
func (t Track) Show(id, rev string) (string, error) {
	if !t.Exists(id) {
		return "", nil
	}
	return t.git(id, "show", "--pretty=format:%ci%n%b", rev, "--numstat", "--patch")
}

// Diff returns the differences of the document between two revisions.
func (t Track) Diff(id, rev1, rev2 string) (string, error) {
	if !t.Exists(id) {
		return "", nil
	}
	return t.git(id, "diff", "--pretty=format:%ci%n%b", rev1, rev2, "--", t.File)
}

// Rollback restores the document of a revision as a new commit.
func (t Track) Rollback(id, rev, author string) error {
	if !t.Exists(id) {
		return fmt.Errorf("no history for %s", id)
	}
	shown, err := t.git(id, "show", "--pretty=format:%ci%n%b", rev, "--patch")
	if err != nil {
		return err
	}
	date := ""
	if first, _, _ := strings.Cut(shown, "\n"); !strings.Contains(first, "diff") {
		date = first
	}
	if err := t.gitIn(id, "checkout", rev, "--", t.File); err != nil {
		return err
	}
	args := append([]string{"commit", "-m", "rollback to " + date}, authorArg(author)...)
	_ = t.gitIn(id, append(args, "-a")...)
	return nil
}

// LogEntry is one commit of an object's history, its message included.
type LogEntry struct {
	ID      string `json:"id"`
	Date    string `json:"date"`
	Author  string `json:"author"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// Log returns the last n commits of the object, newest first, from rev when
// given, from the last commit otherwise.
func (t Track) Log(id string, n int, rev ...string) ([]LogEntry, error) {
	if !t.Exists(id) {
		return []LogEntry{}, nil
	}
	// Fields apart with the unit separator, commits with the record separator: a
	// message may hold anything else.
	args := []string{"log", "-n", strconv.Itoa(n), "--format=%H%x1f%aI%x1f%an <%ae>%x1f%s%x1f%b%x1e"}
	out, err := t.git(id, append(args, rev...)...)
	if err != nil {
		// A repository without a commit yet has no log.
		if _, headErr := t.git(id, "rev-parse", "--verify", "HEAD"); headErr != nil {
			return []LogEntry{}, nil
		}
		return nil, err
	}
	commits := []LogEntry{}
	for _, record := range strings.Split(out, "\x1e") {
		fields := strings.Split(strings.TrimLeft(record, "\n"), "\x1f")
		if len(fields) < 5 {
			continue
		}
		commits = append(commits, LogEntry{
			ID:      fields[0],
			Date:    fields[1],
			Author:  strings.TrimSuffix(fields[2], " <>"),
			Subject: fields[3],
			Body:    strings.TrimSpace(fields[4]),
		})
	}
	return commits, nil
}

// ResolveCommit returns the full id of a commit of the object's history, and
// false when rev names none.
func (t Track) ResolveCommit(id, rev string) (string, bool) {
	if !t.Exists(id) || !ValidRev(rev) {
		return "", false
	}
	out, err := t.git(id, "rev-parse", "--verify", "--quiet", rev+"^{commit}")
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(out), true
}

// Parent returns the commit before rev, and false for the first one.
func (t Track) Parent(id, rev string) (string, bool) {
	return t.ResolveCommit(id, rev+"^")
}

// FileAt returns the document as it was at a commit.
func (t Track) FileAt(id, rev string) (string, error) {
	return t.git(id, "show", rev+":"+t.File)
}

// DiffFile returns the change of the document from one commit to another, as
// git diff prints it; from empty compares with nothing, for a first commit.
func (t Track) DiffFile(id, from, to string) (string, error) {
	if from == "" {
		from = emptyTree
	}
	return t.git(id, "diff", from, to, "--", t.File)
}
