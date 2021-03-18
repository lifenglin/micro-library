package library

import (
	"github.com/percona/go-mysql/query"
	"regexp"
)

func TransferSQLToTpl(sql string) (tpl string, err error) {
	queryTpl := query.Fingerprint(sql)

	re, err := regexp.Compile("([a-zA-Z_]+)(?:\\d+)")
	if nil != err {
		return "", nil
	}

	tpl = re.ReplaceAllString(queryTpl, "${1}xxx")

	return tpl, nil
}