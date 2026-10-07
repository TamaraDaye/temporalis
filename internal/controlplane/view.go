package controlplane

type (
	ServiceID    string
	InstanceID   string
	SubscriberID string
	Instances    map[InstanceID]Instance
)


type RegisterRequest struct {
	Service ServiceID `json:"service"`
	Port    int       `json:"port"`
}

type Instance struct {
	ID       InstanceID `json:"id"`
	Endpoint string     `json:"endpoint"`
}

type View struct {
	Version  uint64                  `json:"version"`
	Services map[ServiceID]Instances `json:"services"`
}

func (i Instances) Add(id InstanceID, instance Instance) {
	i[id] = instance
}

