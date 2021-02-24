package library

import (
	"fmt"
	"github.com/lifenglin/micro-library/helper"
	"golang.org/x/net/context"
	"strings"
	"time"
)

type Fmt struct {
	Distance int64
	Unit int64
	DataType string
}

type DiffText struct {
	Default string
	MonthDat string
	Fmts []*Fmt
}

var diffTextMap map[string]*DiffText

func init() {
	diffTextMap = make(map[string]*DiffText)
}

func newDiffText(ctx context.Context, hlp *helper.Helper, language string) (diffText *DiffText, err error) {
	diffText = new(DiffText)
	diffText.Default, err = LiteralLang(ctx, hlp, "nice.datatype_time.default", language, nil)
	if nil != err {
		return nil, err
	}
	// 转换成golang的时间Format
	diffText.Default = strings.Replace(diffText.Default, "Y", "2006", 1)
	diffText.Default = strings.Replace(diffText.Default, "n", "1", 1)
	diffText.Default = strings.Replace(diffText.Default, "M", "Jan", 1)
	diffText.Default = strings.Replace(diffText.Default, "j", "2", 1)

	diffText.MonthDat, err = LiteralLang(ctx, hlp, "nice.datatype_time.monthDay", language, nil)
	if nil != err {
		return nil, err
	}
	// 转换成golang的时间Format
	diffText.MonthDat = strings.Replace(diffText.MonthDat, "n", "1", 1)
	diffText.MonthDat = strings.Replace(diffText.MonthDat, "M", "Jan", 1)
	diffText.MonthDat = strings.Replace(diffText.MonthDat, "j", "2", 1)

	fmtTemple := new(Fmt)
	fmtTemple.Distance = -172800
	fmtTemple.Unit = 86400
	fmtTemple.DataType, err = LiteralLang(ctx, hlp, "nice.datatype_time.fmts.days", language, nil)
	if nil != err {
		return nil, err
	}
	diffText.Fmts = append(diffText.Fmts, fmtTemple)

	fmtTemple = new(Fmt)
	fmtTemple.Distance = -86400
	fmtTemple.Unit = 86400
	fmtTemple.DataType, err = LiteralLang(ctx, hlp, "nice.datatype_time.fmts.1_day", language, nil)
	if nil != err {
		return nil, err
	}
	diffText.Fmts = append(diffText.Fmts, fmtTemple)

	fmtTemple = new(Fmt)
	fmtTemple.Distance = -7200
	fmtTemple.Unit = 3600
	fmtTemple.DataType, err = LiteralLang(ctx, hlp, "nice.datatype_time.fmts.hours", language, nil)
	if nil != err {
		return nil, err
	}
	diffText.Fmts = append(diffText.Fmts, fmtTemple)

	fmtTemple = new(Fmt)
	fmtTemple.Distance = -60
	fmtTemple.Unit = 60
	fmtTemple.DataType, err = LiteralLang(ctx, hlp, "nice.datatype_time.fmts.minutes", language, nil)
	if nil != err {
		return nil, err
	}
	diffText.Fmts = append(diffText.Fmts, fmtTemple)

	fmtTemple = new(Fmt)
	fmtTemple.Distance = -1
	fmtTemple.Unit = 1
	fmtTemple.DataType, err = LiteralLang(ctx, hlp, "nice.datatype_time.fmts.1_hour", language, nil)
	if nil != err {
		return nil, err
	}
	diffText.Fmts = append(diffText.Fmts, fmtTemple)

	fmtTemple = new(Fmt)
	fmtTemple.Distance = 60
	fmtTemple.Unit = 1
	fmtTemple.DataType, err = LiteralLang(ctx, hlp, "nice.datatype_time.fmts.justnow", language, nil)
	if nil != err {
		return nil, err
	}
	diffText.Fmts = append(diffText.Fmts, fmtTemple)

	fmtTemple = new(Fmt)
	fmtTemple.Distance = 3600
	fmtTemple.Unit = 60
	fmtTemple.DataType, err = LiteralLang(ctx, hlp, "nice.datatype_time.fmts.m_ago", language, nil)
	if nil != err {
		return nil, err
	}
	diffText.Fmts = append(diffText.Fmts, fmtTemple)

	fmtTemple = new(Fmt)
	fmtTemple.Distance = 86400
	fmtTemple.Unit = 3600
	fmtTemple.DataType, err = LiteralLang(ctx, hlp, "nice.datatype_time.fmts.h_ago", language, nil)
	if nil != err {
		return nil, err
	}
	diffText.Fmts = append(diffText.Fmts, fmtTemple)

	fmtTemple = new(Fmt)
	fmtTemple.Distance = 604800
	fmtTemple.Unit = 86400
	fmtTemple.DataType, err = LiteralLang(ctx, hlp, "nice.datatype_time.fmts.d_ago", language, nil)
	if nil != err {
		return nil, err
	}
	diffText.Fmts = append(diffText.Fmts, fmtTemple)

	return diffText, nil
}

func GetDiffText(ctx context.Context, hlp *helper.Helper, goalTime int64, language string) (text string, err error) {
	diffText := diffTextMap[language]

	if nil == diffText {
		diffText, err = newDiffText(ctx, hlp, language)
		if nil != err {
			return "", err
		}
		diffTextMap[language] = diffText
	}

	diffTime := time.Now().Unix() - goalTime

	for _, item := range diffText.Fmts {
		if diffTime < item.Distance {
			diffTimeAbs := diffTime
			if diffTimeAbs < 0 {
				diffTimeAbs = 0 - diffTimeAbs
			}

			if 0 != strings.Count(item.DataType, "%d") {
				text = fmt.Sprintf(item.DataType, diffTimeAbs/item.Unit)
			} else {
				text = item.DataType
			}

			return text, nil
		}
	}

	currentYear := time.Now().Year()
	goalTimeUnix := time.Unix(goalTime, 0)
	goalYear := goalTimeUnix.Year()
	if currentYear == goalYear {
		text = goalTimeUnix.Format(diffText.MonthDat)
		return text, nil
	}

	text = goalTimeUnix.Format(diffText.Default)
	return text, nil
}
