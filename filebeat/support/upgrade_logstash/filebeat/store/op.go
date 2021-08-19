package store

type MapStr map[string]interface{}

type (
	Op interface {
		Name() string
	}

	// OpSet encodes the 'Set' operations in the update log.
	OpSet struct {
		K string
		V MapStr
	}

	// OpRemove encodes the 'Remove' operation in the update log.
	OpRemove struct {
		K string
	}
)

// operation type names
const (
	opValSet    = "set"
	opValRemove = "remove"
)

func (*OpSet) Name() string    { return opValSet }
func (*OpRemove) Name() string { return opValRemove }
