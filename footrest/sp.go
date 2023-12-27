package footrest

type SPParam struct {
	Name      string `json:"name"`
	Direction string `json:"direction"` // in,out,inout,return
	Type      string `json:"type"`
	Value     any    `json:"value"`
}
