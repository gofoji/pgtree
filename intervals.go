package pgtree

type IntervalModType uint32

const (
	Empty IntervalModType = 1 << iota
	Month
	Year
	Day
	Julian
	TZ
	DTZ
	DynTZ
	IgnoreDTF
	AMPM
	Hour
	Minute
	Second
	MilliSecond
	MicroSecond
	DoY
	DoW
	Units
	ADBC
	AGO
	ABSBefore
	ABSAfter
	ISODate
	ISOTime
	Week
	Decade
	Century
	Millennium
	DTZMod
)

func (i IntervalModType) String() string {
	switch i {
	case Month:
		return "month"
	case Year:
		return "year"
	case Day:
		return "day"
	case Hour:
		return "hour"
	case Minute:
		return "minute"
	case Second:
		return "second"
	case Year | Month:
		return "year to month"
	case Hour | Day:
		return "day to hour"
	case Day | Hour | Minute:
		return "day to minute"
	case Day | Hour | Minute | Second:
		return "day to second"
	case Hour | Minute:
		return "hour to minute"
	case Hour | Minute | Second:
		return "hour to second"
	case Minute | Second:
		return "minute to second"
	}

	return ""
}
