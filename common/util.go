package common

func FindStringArrayKeyIndex(array []string, key string) (index int, exist bool) {
	for k, v := range array {
		if v == key {
			return k, true
		}
	}
	return -1, false
}
