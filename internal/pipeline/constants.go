package pipeline

const (
	FetchTasksStream = "stream:fetch_tasks"
	ParseTasksStream = "stream:parse_tasks"

	FetchDLQStream = "stream:fetch_dlq"
	ParseDLQStream = "stream:parse_dlq"

	CollectorGroup = "collector-group"
	ParserGroup    = "parser-group"

	CollectorConsumerPrefix = "collector-"
	ParserConsumerPrefix    = "parser-"

	StreamMaxStreamSize = 100000
)