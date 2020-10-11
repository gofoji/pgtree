package pgtree

import "math/bits"

type IntervalModType uint32

const (
	Month IntervalModType = 1 << iota
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
	case 0:
		return ""
	case Month:
		return "month"
	case Year:
		return "year"
	case Day:
		return "day"
	case Julian:
		return ""
	case TZ:
		return ""
	case DTZ:
		return ""
	case DynTZ:
		return ""
	case IgnoreDTF:
		return ""
	case AMPM:
		return ""
	case Hour:
		return "hour"
	case Minute:
		return "minute"
	case Second:
		return "second"
	case MilliSecond:
		return "millisecond"
	case MicroSecond:
		return "microsecond"
	case DoY:
		return ""
	case DoW:
		return ""
	case Units:
		return ""
	case ADBC:
		return ""
	case AGO:
		return ""
	case ABSBefore:
		return ""
	case ABSAfter:
		return ""
	case ISODate:
		return ""
	case ISOTime:
		return ""
	case Week:
		return ""
	case Decade:
		return ""
	case Century:
		return ""
	case Millennium:
		return ""
	case DTZMod:
		return ""
	}
	return i.Low().String() + " to " + i.High().String()
}

func (i IntervalModType) High() IntervalModType {
	return IntervalModType(1 << (30 - bits.LeadingZeros32(uint32(i))))
}

func (i IntervalModType) Low() IntervalModType {
	return IntervalModType(1 << (bits.TrailingZeros32(uint32(i)) - 1))
}
