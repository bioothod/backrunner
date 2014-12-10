package bucket

import (
	"github.com/bioothod/elliptics-go/elliptics"
	"log"
	"sort"
	"time"
)

type bstat struct {
	bucket			*Bucket
	ab			elliptics.AddressBackend
	free_space_rate		float64
	removed_space_rate	float64
}

type bstat_sorter struct {
	defrag			[]bstat
}

func (stats bstat_sorter) Len() int {
	return len(stats.defrag)
}

func (stats bstat_sorter) Swap(i, j int) {
	stats.defrag[i], stats.defrag[j] = stats.defrag[j], stats.defrag[i]
}

func (stats bstat_sorter) Less(i, j int) bool {
	return stats.defrag[i].removed_space_rate < stats.defrag[j].removed_space_rate
}

// called without locks
func (bctl *BucketCtl) DefragBuckets(defrag_buckets []bstat) {
	if len(defrag_buckets) <= 0 {
		return
	}

	sorter := bstat_sorter {
		defrag: defrag_buckets,
	}
	sort.Sort(sorter)

	db := make(map[*Bucket]int)
	addrs := make(map[elliptics.RawAddr]int)

	s, err := elliptics.NewSession(bctl.e.Node)
	if err != nil {
		log.Printf("defag: could not create new session: %v\n", err)
		return
	}

	for idx := len(defrag_buckets) - 1; idx >= 0; idx-- {
		b := &defrag_buckets[idx]

		addrs[b.ab.Addr]++
		defrag_backends_on_storage := addrs[b.ab.Addr]

		if defrag_backends_on_storage <= bctl.Conf.Proxy.DefragMaxBackendsPerServer {
			log.Printf("defrag: starting defragmentation in bucket: %s, %s, free-space-rate: %f, removed-space-rate: %f\n",
						b.bucket.Name, b.ab.String(), b.free_space_rate, b.removed_space_rate)

			for _ = range s.BackendStartDefrag(b.ab.Addr.DnetAddr(), b.ab.Backend) {
			}

			db[b.bucket]++
			if len(db) >= bctl.Conf.Proxy.DefragMaxBuckets {
				break
			}
		}
	}

	return
}

func (bctl *BucketCtl) ScanBuckets() {
	if time.Since(bctl.DefragTime).Seconds() <= 30 {
		return
	}

	bctl.Lock()

	bctl.DefragTime = time.Now()
	log.Printf("defrag: starting defrag scanning\n")

	defrag_buckets := make([]bstat, 0)

	for _, b := range bctl.AllBuckets() {
		groups_defrag_already_running := 0

		for _, stat_group := range b.Group {
			backends_defrag_already_running := 0

			for _, st := range stat_group.Ab {
				// allow at most half of the groups to have backends being defragmented
				// this allows clients to read data from groups where there is no defragmentation process
				if st.DefragState != 0 {
					if backends_defrag_already_running == 0 {
						groups_defrag_already_running++
					}
					backends_defrag_already_running++

					if groups_defrag_already_running * 2 > len(b.Group) {
						break
					}
				}
			}

			// do not allow to run defragmentation in bucket where
			// more than a half of its groups are 'dirty', i.e. where defragmentation process is already running
			if groups_defrag_already_running * 2 > len(b.Group) {
				break
			}
		}

		if groups_defrag_already_running * 2 > len(b.Group) {
			continue
		}

		for _, stat_group := range b.Group {
			for ab, st := range stat_group.Ab {
				if st.RO {
					log.Printf("defrag: bucket: %s, %s: backend is in read-only mode\n",
						b.Name, ab.String())
					continue
				}

				free_space_rate := 1.0 - float64(st.VFS.BackendUsedSize) / float64(st.VFS.TotalSizeLimit)
				if free_space_rate > bctl.Conf.Proxy.DefragFreeSpaceLimit {
					log.Printf("defrag: bucket: %s, %s, free-space-rate: %f, must be < %f\n",
						b.Name, ab.String(), free_space_rate, bctl.Conf.Proxy.DefragFreeSpaceLimit)
					continue
				}

				removed_space_rate := float64(st.VFS.BackendRemovedSize) / float64(st.VFS.TotalSizeLimit)
				if removed_space_rate < bctl.Conf.Proxy.DefragRemovedSpaceLimit {
					log.Printf("defrag: bucket: %s, %s, free-space-rate: %f, removed-space-rate: %f, must be > %f\n",
						b.Name, ab.String(), free_space_rate,
						removed_space_rate, bctl.Conf.Proxy.DefragRemovedSpaceLimit)
					continue
				}

				bs := bstat {
					bucket:			b,
					ab:			ab,
					free_space_rate:	free_space_rate,
					removed_space_rate:	removed_space_rate,
				}

				log.Printf("defrag: added bucket: %s, %s, free-space-rate: %f, removed-space-rate: %f\n",
					b.Name, ab.String(), free_space_rate, removed_space_rate)

				defrag_buckets = append(defrag_buckets, bs)
			}
		}
	}

	bctl.Unlock()

	bctl.DefragBuckets(defrag_buckets)

	return
}
