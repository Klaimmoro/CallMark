package labeling

import "fmt"

const (
	LabelLegit      = "legit"
	LabelSpam       = "spam"
	LabelSuspicious = "suspicious"
)

// Label вычисляет метку звонка на основе ответа Rules Service.
// Чистая функция: тот же вход всегда даёт тот же выход, никаких обращений
// к сети или базе данных — именно это делает её тривиально тестируемой
// table-driven тестами, без Docker и без моков.
func Label(result RuleResult) (label string, reason string) {
	switch result.Status {
	case "OK":
		return LabelLegit, fmt.Sprintf("caller verified, category=%s", result.Category)
	case "BLACKLISTED":
		return LabelSpam, fmt.Sprintf("caller is blacklisted, category=%s", result.Category)
	case "UNKNOWN":
		return LabelSuspicious, "caller not found in registry"
	default:
		return LabelSuspicious, fmt.Sprintf("unrecognized rules status: %q", result.Status)
	}
}
