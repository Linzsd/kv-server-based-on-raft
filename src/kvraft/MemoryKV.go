package kvraft

type MemoryKV struct {
	Data map[string]string
}

func (mkv *MemoryKV) put(key string, value string) {
	mkv.Data[key] = value
}

func (mkv *MemoryKV) get(key string) string {
	return mkv.Data[key]
}

func (mkv *MemoryKV) append(key string, value string) string {
	originVal := mkv.Data[key]
	mkv.Data[key] = originVal + value
	return mkv.Data[key]
}
