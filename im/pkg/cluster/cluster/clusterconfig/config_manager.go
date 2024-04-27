package clusterconfig

import (
	"chat/im/pkg/cluster/cluster/clusterconfig/pb"
	"chat/im/pkg/logger"
	"chat/im/pkg/util"
	"go.uber.org/zap"
	"io"
	"os"
	"path"
	"sync"
	"time"
)

type ConfigManager struct {
	sync.RWMutex
	cfg     *pb.Config
	cfgFile *os.File
	opts    *Options
}

func NewConfigManager(opts *Options) *ConfigManager {
	cm := &ConfigManager{
		cfg: &pb.Config{
			SlotCount: opts.SlotCount,
		},
		opts: opts,
	}
	configDir := path.Dir(opts.ConfigPath)
	if configDir != "" {
		err := os.MkdirAll(configDir, os.ModePerm)
		if err != nil {
			logger.Panic("create config dir error", err)
		}
	}
	err := cm.initConfigFromFile()
	if err != nil {
		logger.Panic("init cluster config from file error", zap.Error(err))
	}

	opts.AppliedConfigVersion = cm.cfg.Version

	return cm
}

func (c *ConfigManager) initConfigFromFile() error {
	clusterCfgPath := c.opts.ConfigPath
	var err error
	c.cfgFile, err = os.OpenFile(clusterCfgPath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		logger.Panic("Open cluster config file failed!", err)
	}
	data, err := io.ReadAll(c.cfgFile)
	if err != nil {
		logger.Panic("Read cluster config file failed!", err)
	}
	if len(data) > 0 {
		if err := util.ReadJSONByByte(data, c.cfg); err != nil {
			logger.Panic("Unmarshal cluster config failed!", err)
		}
	}
	return nil
}

func (c *ConfigManager) GetConfig() *pb.Config {
	return c.cfg
}

func (c *ConfigManager) GetConfigDataByCfg(cfg *pb.Config) []byte {
	return []byte(util.ToJSON(cfg))
}

func (c *ConfigManager) GetConfigData() []byte {
	c.Lock()
	defer c.Unlock()
	return c.getConfigData()
}

func (c *ConfigManager) getConfigData() []byte {
	return []byte(util.ToJSON(c.cfg))
}

func (c *ConfigManager) UnmarshalConfigData(data []byte, cfg *pb.Config) error {
	c.Lock()
	defer c.Unlock()
	return util.ReadJSONByByte(data, cfg)
}

func (c *ConfigManager) UpdateConfig(cfg *pb.Config) error {
	c.Lock()
	defer c.Unlock()
	c.cfg = cfg
	return c.saveConfig()
}

func (c *ConfigManager) saveConfig() error {
	data := c.getConfigData()
	err := c.cfgFile.Truncate(0)
	if err != nil {
		return err
	}
	if _, err := c.cfgFile.WriteAt(data, 0); err != nil {
		return err
	}
	return nil
}

func (c *ConfigManager) AddOrUpdateNodes(nodes []*pb.Node, cfg *pb.Config) {
	c.Lock()
	defer c.Unlock()
	for i, node := range nodes {
		if c.existNodeByCfg(node.Id, cfg) {
			cfg.Nodes[i] = node
			continue
		}
		cfg.Nodes = append(cfg.Nodes, node)
	}
}

func (c *ConfigManager) existNodeByCfg(nodeId uint64, cfg *pb.Config) bool {
	for _, node := range cfg.Nodes {
		if node.Id == nodeId {
			return true
		}
	}
	return false
}

func (c *ConfigManager) AddOrUpdateSlots(slots []*pb.Slot, cfg *pb.Config) {
	c.Lock()
	defer c.Unlock()
	for i, slot := range slots {
		if c.existSlotByCfg(slot.Id, cfg) {
			cfg.Slots[i] = slot
			continue
		}
		cfg.Slots = append(cfg.Slots, slot)
	}
}

func (c *ConfigManager) existSlotByCfg(slotId uint32, cfg *pb.Config) bool {
	for _, slot := range cfg.Slots {
		if slot.Id == slotId {
			return true
		}
	}
	return false
}

func (c *ConfigManager) NodeIsOnline(id uint64) bool {
	c.RLock()
	defer c.RUnlock()
	for _, node := range c.cfg.Nodes {
		if node.Id == id {
			return node.Online
		}
	}
	return false
}

func (c *ConfigManager) SetNodeOnline(nodeId uint64, online bool, cfg *pb.Config) {
	c.Lock()
	defer c.Unlock()
	for _, node := range cfg.Nodes {
		if node.Id == nodeId {
			node.Online = online
			if !online {
				node.OfflineCount++
				node.LastOffline = time.Now().Unix()
			}
			break
		}
	}
}
