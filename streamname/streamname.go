package streamname

import (
	"fmt"
	"strings"
)

func StreamName(categoryName, id string, types ...string) string {
	typeList := strings.Join(types, "+")

	streamName := categoryName
	if typeList != "" {
		streamName = fmt.Sprintf("%s:%s", streamName, typeList)
	}
	if id != "" {
		streamName = fmt.Sprintf("%s-%s", streamName, id)
	}
	return streamName
}

func ID(streamName string) string {
	parts := strings.SplitN(streamName, "-", 2)
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}

func Category(streamName string) string {
	return strings.SplitN(streamName, "-", 2)[0]
}

func IsCategory(streamName string) bool {
	return !strings.ContainsRune(streamName, '-')
}

func TypeList(streamName string) string {
	parts := strings.SplitN(Category(streamName), ":", 2)
	if len(parts) != 2 {
		return ""
	}
	typeList := parts[1]
	if strings.HasPrefix(streamName, typeList) {
		return ""
	}
	return typeList
}

func Types(streamName string) []string {
	typeList := TypeList(streamName)
	if typeList == "" {
		return nil
	}
	return strings.Split(typeList, "+")
}

func EntityName(streamName string) string {
	return strings.SplitN(Category(streamName), ":", 2)[0]
}
