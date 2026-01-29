package feederhandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/feeder"
)

func (f *Feeder) PostFeedInstanceStatus(c echo.Context, params feeder.PostFeedInstanceStatusParams) error {
	var (
		keyH        = cachekeys.FeedInstanceStatusH
		keyQ        = cachekeys.FeedInstanceStatusQ
		keyPendingH = cachekeys.FeedInstanceStatusPendingH

		syncMode bool
		pubsub   *redis.PubSub
		doneC    chan any

		// timeout is the default timeout for api calls
		timeout = 2 * time.Second
	)

	log := getLog(c)

	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		log.Debug("node auth problem")
		return JSONNodeAuthProblem(c)
	}

	clusterID := clusterIDFromContext(c)
	if clusterID == "" {
		return JSONProblemf(c, http.StatusConflict, "Refused", "authenticated node doesn't define cluster id")
	}

	var payload feeder.PostFeedInstanceStatusJSONRequestBody
	if err := c.Bind(&payload); err != nil {
		return JSONProblem(c, http.StatusBadRequest, "Failed to json decode request body", err.Error())
	}

	if params.Sync != nil && *params.Sync {
		syncMode = true
		if f.SyncTimeout > 0 {
			timeout = f.SyncTimeout
		}
	}

	if !strings.HasPrefix(payload.Version, "2.") {
		log.Error(fmt.Sprintf("unexpected version %s", payload.Version))
		return JSONProblemf(c, http.StatusBadRequest, "BadRequest", "unsupported data client version: %s", payload.Version)
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return JSONProblem(c, http.StatusInternalServerError, "Failed to re-encode config", err.Error())
	}

	id := fmt.Sprintf("%s@%s@%s", payload.Path, nodeID, clusterID)

	ctx, cancel := context.WithTimeout(c.Request().Context(), timeout)
	defer cancel()

	if syncMode {
		pubsub = f.Redis.Subscribe(ctx, cachekeys.FeedInstanceStatusP)
		defer func() {
			if err := pubsub.Close(); err != nil {
				log.Error(fmt.Sprintf("can't close subscription from %s: %s", cachekeys.FeedInstanceStatusP, err))
			}
		}()
		if i, err := pubsub.Receive(ctx); err != nil {
			return JSONProblemf(c, http.StatusInternalServerError, "redis subscription", "can't subscribe to %s: %s", cachekeys.FeedInstanceStatusP, err)
		} else {
			switch i.(type) {
			case *redis.Subscription:
			default:
				return JSONProblemf(c, http.StatusInternalServerError, "redis subscription", "unexpected message type %T", i)
			}
		}
		doneC = make(chan any)
		go func() {
			// Wait for confirmation that the data has been stored
			for {
				// Ensure return on ctx done
				select {
				case <-ctx.Done():
					return
				default:
				}

				if i, err := pubsub.Receive(ctx); err != nil {
					log.Debug(fmt.Sprintf("can't receive from %s: %s", cachekeys.FeedInstanceStatusP, err))
					return
				} else {
					switch m := i.(type) {
					case *redis.Message:
						if m == nil {
							continue
						} else if m.Payload == id {
							log.Debug(fmt.Sprintf("got from %s: %s", cachekeys.FeedInstanceStatusP, id))
							doneC <- nil
							return
						}
					}
				}
			}
		}()
	}

	// Store data in Redis hash with generated ID as key
	s := fmt.Sprintf("HSET %s %s", keyH, id)

	if _, err := f.Redis.HSet(ctx, keyH, id, b).Result(); err != nil {
		s = fmt.Sprintf("%s: %s", s, err)
		log.Error(s)
		return JSONProblem(c, http.StatusInternalServerError, "", s)
	}

	if err := f.pushNotPending(ctx, keyPendingH, keyQ, id); err != nil {
		log.Error(fmt.Sprintf("can't push %s %s: %s", keyQ, id, err))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't push %s %s: %s", keyQ, id, err)
	}

	if syncMode {
		select {
		case <-doneC:
			return c.JSON(http.StatusOK, nil)
		case <-ctx.Done():
			return c.JSON(http.StatusAccepted, nil)
		}
	} else {
		return c.JSON(http.StatusAccepted, nil)
	}
}
