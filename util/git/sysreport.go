package git

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// Sysreport reads the history of the files and command outputs the agents
// report, kept by the scheduler in one git repository per node under Dir: the
// tracked files under file/, the outputs of the tracked commands under cmd/,
// each command line encoded as a file name.
type Sysreport struct {
	Dir string
}

// ErrNoSysreport is returned for a node that never reported, ErrNoRevision for
// a revision its history does not hold.
var (
	ErrNoSysreport = errors.New("no sysreport")
	ErrNoRevision  = errors.New("no such revision")
)

// nodeIDPattern bounds the node ids used as directory names.
var nodeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]*$`)

// oidPattern bounds the object ids passed to git.
var oidPattern = regexp.MustCompile(`^[0-9a-f]{7,64}$`)

// ValidOID reports whether s may be passed to git as an object id.
func ValidOID(s string) bool { return oidPattern.MatchString(s) }

func (s Sysreport) git(nodeID string, args ...string) (string, error) {
	if !nodeIDPattern.MatchString(nodeID) {
		return "", ErrNoSysreport
	}
	gitDir := filepath.Join(s.Dir, nodeID, ".git")
	if _, err := os.Stat(gitDir); err != nil {
		return "", ErrNoSysreport
	}
	// quotepath off: paths are printed as they are, not octal-escaped.
	cmd := exec.Command("git", append([]string{"--git-dir=" + gitDir, "-c", "core.quotepath=off"}, args...)...)
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(errOut.String()))
	}
	return out.String(), nil
}

// SysreportFileStat is a file changed by a revision, with its line counts.
type SysreportFileStat struct {
	// Path is the path in the repository: file/etc/hosts, cmd/ip(space)route.
	Path    string
	Added   int
	Deleted int
	Binary  bool
}

// SysreportChange is a revision of a node's sysreport.
type SysreportChange struct {
	ID   string
	Date string
	// Initial is set for the first report of the node, which adds every file.
	Initial bool
	Files   []SysreportFileStat
}

// sysreportTimelineCap bounds the revisions read: a node reports once a day at
// most, this is years of changes.
const sysreportTimelineCap = 2000

// Timeline returns the revisions of the node, newest first, optionally bounded
// by dates git understands (since, until).
func (s Sysreport) Timeline(nodeID, since, until string) ([]SysreportChange, error) {
	args := []string{"log", "-n", strconv.Itoa(sysreportTimelineCap), "--no-renames", "--numstat", "--format=%x01%H%x09%cI%x09%P"}
	if since != "" {
		args = append(args, "--since="+since)
	}
	if until != "" {
		args = append(args, "--until="+until)
	}
	out, err := s.git(nodeID, args...)
	if err != nil {
		return nil, err
	}
	var changes []SysreportChange
	for _, chunk := range strings.Split(out, "\x01")[1:] {
		lines := strings.Split(chunk, "\n")
		head := strings.Split(lines[0], "\t")
		if len(head) < 2 {
			continue
		}
		change := SysreportChange{ID: head[0], Date: head[1], Initial: len(head) < 3 || head[2] == ""}
		for _, line := range lines[1:] {
			if stat, ok := parseNumstat(line); ok {
				change.Files = append(change.Files, stat)
			}
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func parseNumstat(line string) (SysreportFileStat, bool) {
	fields := strings.SplitN(line, "\t", 3)
	if len(fields) != 3 {
		return SysreportFileStat{}, false
	}
	stat := SysreportFileStat{Path: unquotePath(fields[2])}
	if fields[0] == "-" {
		stat.Binary = true
		return stat, true
	}
	stat.Added, _ = strconv.Atoi(fields[0])
	stat.Deleted, _ = strconv.Atoi(fields[1])
	return stat, true
}

// unquotePath undoes the quoting git keeps for the paths holding a quote, a
// backslash or a control character.
func unquotePath(p string) string {
	if strings.HasPrefix(p, `"`) {
		if u, err := strconv.Unquote(p); err == nil {
			return u
		}
	}
	return p
}

// SysreportFileDiff is the change a revision made to a file, as a unified diff
// starting at its first hunk.
type SysreportFileDiff struct {
	SysreportFileStat
	Diff string
	// Truncated is set when the diff was cut at the size limit.
	Truncated bool
}

// sysreportDiffCap bounds the diff returned for one file.
const sysreportDiffCap = 256 * 1024

// Show returns the date of a revision and the change it made to each file.
func (s Sysreport) Show(nodeID, rev string) (string, []SysreportFileDiff, error) {
	if !ValidRev(rev) {
		return "", nil, ErrNoRevision
	}
	out, err := s.git(nodeID, "show", "--no-renames", "--no-color", "-U3", "--format=%x01%cI", rev)
	if err != nil {
		if errors.Is(err, ErrNoSysreport) {
			return "", nil, err
		}
		return "", nil, ErrNoRevision
	}
	_, body, _ := strings.Cut(out, "\x01")
	date, patch, _ := strings.Cut(body, "\n")
	return strings.TrimSpace(date), parsePatch(patch), nil
}

func parsePatch(patch string) []SysreportFileDiff {
	var diffs []SysreportFileDiff
	for _, block := range strings.Split("\n"+patch, "\ndiff --git ")[1:] {
		lines := strings.Split(block, "\n")
		var d SysreportFileDiff
		hunk := -1
		for i, line := range lines {
			switch {
			case strings.HasPrefix(line, "--- a/") && d.Path == "":
				d.Path = unquotePath(strings.TrimSuffix(line[len("--- a/"):], "\t"))
			case strings.HasPrefix(line, "+++ b/"):
				d.Path = unquotePath(strings.TrimSuffix(line[len("+++ b/"):], "\t"))
			case strings.HasPrefix(line, "Binary files "):
				d.Binary = true
			}
			if strings.HasPrefix(line, "@@") {
				hunk = i
				break
			}
		}
		if d.Path == "" {
			// No ---/+++ line (binary or mode change): the header names the path twice.
			if header := lines[0]; strings.HasPrefix(header, "a/") {
				if at := strings.Index(header, " b/"); at > 0 {
					d.Path = unquotePath(header[2:at])
				}
			}
		}
		if d.Path == "" {
			continue
		}
		if hunk >= 0 {
			for _, line := range lines[hunk:] {
				switch {
				case strings.HasPrefix(line, "+"):
					d.Added++
				case strings.HasPrefix(line, "-"):
					d.Deleted++
				}
			}
			d.Diff = strings.TrimRight(strings.Join(lines[hunk:], "\n"), "\n")
			if len(d.Diff) > sysreportDiffCap {
				d.Diff = d.Diff[:strings.LastIndexByte(d.Diff[:sysreportDiffCap], '\n')+1]
				d.Truncated = true
			}
		}
		diffs = append(diffs, d)
	}
	return diffs
}

// SysreportEntry is a file of the node's sysreport at a revision.
type SysreportEntry struct {
	OID  string
	Path string
	Size int64
}

// Tree lists the files of the node's sysreport at a revision.
func (s Sysreport) Tree(nodeID, rev string) ([]SysreportEntry, error) {
	if !ValidRev(rev) {
		return nil, ErrNoRevision
	}
	out, err := s.git(nodeID, "ls-tree", "-r", "--long", rev)
	if err != nil {
		if errors.Is(err, ErrNoSysreport) {
			return nil, err
		}
		return nil, ErrNoRevision
	}
	var entries []SysreportEntry
	for _, line := range strings.Split(out, "\n") {
		meta, path, ok := strings.Cut(line, "\t")
		fields := strings.Fields(meta)
		if !ok || len(fields) != 4 || fields[1] != "blob" {
			continue
		}
		size, _ := strconv.ParseInt(fields[3], 10, 64)
		entries = append(entries, SysreportEntry{OID: fields[2], Path: unquotePath(path), Size: size})
	}
	return entries, nil
}

// sysreportFileCap bounds the content returned for one file.
const sysreportFileCap = 1024 * 1024

// Blob returns the content of an object, cut at the size limit.
func (s Sysreport) Blob(nodeID, oid string) (content []byte, truncated bool, err error) {
	if !ValidOID(oid) {
		return nil, false, ErrNoRevision
	}
	out, err := s.git(nodeID, "cat-file", "blob", oid)
	if err != nil {
		if errors.Is(err, ErrNoSysreport) {
			return nil, false, err
		}
		return nil, false, ErrNoRevision
	}
	if len(out) > sysreportFileCap {
		return []byte(out[:sysreportFileCap]), true, nil
	}
	return []byte(out), false, nil
}

// commandDecoder undoes the encoding of a command line into a file name, as the
// agent and the historical collector's beautify_fpath() agree on.
var commandDecoder = strings.NewReplacer(
	"(space)", " ", "(pipe)", "|", "(amp)", "&", "(dollar)", "$", "(caret)", "^",
	"(slash)", "/", "(bslash)", `\`, "(colon)", ":", "(semicolon)", ";", "(lt)", "<",
	"(gt)", ">", "(eq)", "=", "(question)", "?", "(at)", "@", "(excl)", "!",
	"(num)", "#", "(pct)", "%", "(dquote)", `"`, "(squote)", "'",
)

// SysreportDisplay turns a path of the repository into what a reader expects:
// the command line of a tracked command, the absolute path of a tracked file.
func SysreportDisplay(path string) (display, kind string) {
	if rest, ok := strings.CutPrefix(path, "cmd/"); ok {
		return commandDecoder.Replace(rest), "command"
	}
	if rest, ok := strings.CutPrefix(path, "file/"); ok {
		return "/" + strings.TrimLeft(rest, "/"), "file"
	}
	return path, "file"
}
