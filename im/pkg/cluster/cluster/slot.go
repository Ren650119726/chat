package cluster

import "fmt"

func GetSlotShardNo(slotID uint32) string {
	return fmt.Sprintf("slot-%d", slotID)
}
