package tool

import (
	"github.com/xuri/excelize/v2"
	"time"
)

type Fields []struct {
	Field  string
	Header string
}

var Lines = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
	"AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ", "AK", "AL", "AM", "AN", "AO", "AP", "AQ", "AR", "AS", "AT", "AU", "AV", "AW", "AX", "AY", "AZ",
	"BA", "BB", "BC", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BK", "BL", "BM", "BN", "BO", "BP", "BQ", "BR", "BS", "BT", "BU", "BV", "BW", "BX", "BY", "BZ",
	"CA", "CB", "CC", "CD", "CE", "CF", "CG", "CH", "CI", "CJ", "CK", "CL", "CM", "CN", "CO", "CP", "CQ", "CR", "CS", "CT", "CU", "CV", "CW", "CX", "CY", "CZ",
	"DA", "DB", "DC", "DD", "DE", "DF", "DG", "DH", "DI", "DJ", "DK", "DL", "DM", "DN", "DO", "DP", "DQ", "DR", "DS", "DT", "DU", "DV", "DW", "DX", "DY", "DZ",
	"EA", "EB", "EC", "ED", "EE", "EF", "EG", "EH", "EI", "EJ", "EK", "EL", "EM", "EN", "EO", "EP", "EQ", "ER", "ES", "ET", "EU", "EV", "EW", "EX", "EY", "EZ"}

func ExcelColumn(row, line int) string {
	if line > len(Lines) {
		return ""
	}
	return Lines[line-1] + Int2String(row)
}

func ExcelExport(data []map[string]interface{}, fields Fields, title string) (file, name string, err error) {
	return ExcelExportPro(data, fields, title, `static/uploads/temp`)
}

func ExcelExportPro(data []map[string]interface{}, fields Fields, title, filePath string) (file, name string, err error) {
	if title == "" {
		title = "Excel数据导出"
	}
	f := excelize.NewFile()
	_ = f.SetSheetName(f.GetSheetName(f.GetActiveSheetIndex()), title)
	_ = f.SetColWidth(title, "A", "XFD", 24)
	fmap := map[string]int{}
	for i, field := range fields {
		_ = f.SetCellValue(title, ExcelColumn(1, i+1), field.Header)
		fmap[field.Field] = i + 1
	}
	for i, d := range data {
		for k, v := range fmap {
			_ = f.SetCellValue(title, ExcelColumn(i+2, v), d[k])
		}
	}
	md := MD5(title + Int64ToString(time.Now().UnixNano()) + Random(10))
	filename := filePath + "/" + md[:2] + "/" + md[2:] + ".xlsx"
	if err = MkDirAll(DirName(filename)); err != nil {
		return "", "", err
	}
	if err = f.SaveAs(filename); err != nil {
		return "", "", err
	} else {
		return filename, title + Date("_Y-m-d_H-i-s") + ".xlsx", nil
	}
}

func ExcelImport(file string, fields Fields) ([]map[string]string, error) {
	data := make([]map[string]string, 0)
	f, err := excelize.OpenFile(file)
	if err != nil {
		return data, err
	}
	rows, err2 := f.GetRows(f.GetSheetName(f.GetActiveSheetIndex()))
	if err2 != nil || len(rows) <= 1 {
		return data, err2
	}
	fmap := map[string]int{}
	for k, row := range rows {
		if k == 0 {
			for k, v := range row {
				fmap[v] = k + 1
			}
			for _, field := range fields {
				fmap[field.Field] = fmap[field.Header]
			}
		} else {
			r := map[string]string{}
			for _, field := range fields {
				line := fmap[field.Field]
				if line == 0 || line > len(row) {
					r[field.Field] = ""
				} else {
					r[field.Field] = row[line-1]
				}
			}
			data = append(data, r)
		}
	}
	return data, nil
}
