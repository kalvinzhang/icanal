package icanal

func contains[T comparable](collection []T, element T) bool {
	for i := range collection {
		if collection[i] == element {
			return true
		}
	}

	return false
}
