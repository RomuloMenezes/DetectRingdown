Sub GeraGrafRingdown()
'
' GeraGrafRingdown Macro
'

'
    Rows("1:1").Select
    Selection.Insert Shift:=xlDown, CopyOrigin:=xlFormatFromLeftOrAbove
    Range("A1").Select
    ActiveCell.FormulaR1C1 = "PMU"
    Range("B1").Select
    ActiveCell.FormulaR1C1 = "Timestamp"
    Range("C1").Select
    ActiveCell.FormulaR1C1 = "Severidade"
    Range("B2").Select
    Range(Selection, Selection.End(xlDown)).Select
    Selection.NumberFormat = "hh:mm:ss.000"
    ActiveWindow.SmallScroll Down:=-12
    Range("A1:C1").Select
    Range(Selection, Selection.End(xlDown)).Select
    Selection.AutoFilter
    ActiveWorkbook.Worksheets("20140204_20141127_04_5").AutoFilter.Sort.SortFields. _
        Clear
    ActiveWorkbook.Worksheets("20140204_20141127_04_5").AutoFilter.Sort.SortFields. _
        Add Key:=Range("A2:A182"), SortOn:=xlSortOnValues, Order:=xlAscending, _
        DataOption:=xlSortNormal
    ActiveWorkbook.Worksheets("20140204_20141127_04_5").AutoFilter.Sort.SortFields. _
        Add Key:=Range("B2:B182"), SortOn:=xlSortOnValues, Order:=xlAscending, _
        DataOption:=xlSortNormal
    With ActiveWorkbook.Worksheets("20140204_20141127_04_5").AutoFilter.Sort
        .Header = xlYes
        .MatchCase = False
        .Orientation = xlTopToBottom
        .SortMethod = xlPinYin
        .Apply
    End With
    Columns("A:A").EntireColumn.AutoFit
    Columns("B:B").EntireColumn.AutoFit
    Columns("C:C").EntireColumn.AutoFit
    Sheets.Add After:=ActiveSheet
    Sheets("Plan1").Select
    Sheets("Plan1").Name = "Grupos de Severidade"
    Sheets("20140204_20141127_04_5").Select
    Sheets("20140204_20141127_04_5").Name = "Dados Originais"
    ActiveSheet.Range("$A$1:$C$182").AutoFilter Field:=3, Criteria1:="1"
    Range("B14").Select
    Range(Selection, Selection.End(xlDown)).Select
    Selection.Copy
    Sheets("Grupos de Severidade").Select
    Range("A2").Select
    ActiveSheet.Paste
    Columns("A:A").EntireColumn.AutoFit
    Sheets("Dados Originais").Select
    Range("A14").Select
    Range(Selection, Selection.End(xlDown)).Select
    Application.CutCopyMode = False
    Selection.Copy
    Sheets("Grupos de Severidade").Select
    Range("C2").Select
    ActiveSheet.Paste
    ActiveSheet.Previous.Select
    Range("C14").Select
    Range(Selection, Selection.End(xlDown)).Select
    Application.CutCopyMode = False
    Selection.Copy
    Sheets("Grupos de Severidade").Select
    Range("D2").Select
    ActiveSheet.Paste
    Range("A1:D1").Select
    Application.CutCopyMode = False
    With Selection
        .HorizontalAlignment = xlCenter
        .VerticalAlignment = xlBottom
        .WrapText = False
        .Orientation = 0
        .AddIndent = False
        .IndentLevel = 0
        .ShrinkToFit = False
        .ReadingOrder = xlContext
        .MergeCells = False
    End With
    Selection.Merge
    ActiveCell.FormulaR1C1 = "De 5 a 10 segundos"
    Range("A2").Select
    Columns("A:A").EntireColumn.AutoFit
    Columns("C:C").EntireColumn.AutoFit
    Columns("D:D").EntireColumn.AutoFit
    Columns("E:E").Select
    Selection.ColumnWidth = 3
    Sheets("Dados Originais").Select
    ActiveSheet.Range("$A$1:$C$182").AutoFilter Field:=3, Criteria1:="2"
    Range("A3").Select
    Range(Selection, Selection.End(xlDown)).Select
    Selection.Copy
    Sheets("Grupos de Severidade").Select
    Range("H2").Select
    ActiveSheet.Paste
    ActiveSheet.Previous.Select
    Range("B3").Select
    Range(Selection, Selection.End(xlDown)).Select
    Application.CutCopyMode = False
    Selection.Copy
    ActiveSheet.Next.Select
    Range("F2").Select
    ActiveSheet.Paste
    ActiveSheet.Previous.Select
    Range("C3").Select
    Range(Selection, Selection.End(xlDown)).Select
    Application.CutCopyMode = False
    Selection.Copy
    ActiveSheet.Next.Select
    Range("I2").Select
    ActiveSheet.Paste
    Range("F1:I1").Select
    Application.CutCopyMode = False
    With Selection
        .HorizontalAlignment = xlCenter
        .VerticalAlignment = xlBottom
        .WrapText = False
        .Orientation = 0
        .AddIndent = False
        .IndentLevel = 0
        .ShrinkToFit = False
        .ReadingOrder = xlContext
        .MergeCells = False
    End With
    Selection.Merge
    ActiveCell.FormulaR1C1 = "De 10 a 15 segundos"
    Range("F2").Select
    Columns("F:F").EntireColumn.AutoFit
    Columns("H:H").EntireColumn.AutoFit
    Columns("I:I").EntireColumn.AutoFit
    Columns("J:J").Select
    Selection.ColumnWidth = 3
    Sheets("Dados Originais").Select
    ActiveSheet.Range("$A$1:$C$182").AutoFilter Field:=3
    ActiveSheet.Range("$A$1:$C$182").AutoFilter Field:=3, Criteria1:=">2", _
        Operator:=xlAnd
    Range("B2").Select
    Range(Selection, Selection.End(xlDown)).Select
    Selection.Copy
    ActiveSheet.Next.Select
    Range("K2").Select
    ActiveSheet.Paste
    ActiveSheet.Previous.Select
    Range("A2").Select
    Range(Selection, Selection.End(xlDown)).Select
    Range("A2:A182").Select
    Application.CutCopyMode = False
    Selection.Copy
    ActiveSheet.Next.Select
    Range("M2").Select
    ActiveSheet.Paste
    ActiveSheet.Previous.Select
    Range("C2").Select
    Range(Selection, Selection.End(xlDown)).Select
    Application.CutCopyMode = False
    Selection.Copy
    ActiveSheet.Next.Select
    Range("N2").Select
    ActiveSheet.Paste
    Range("K1:N1").Select
    Application.CutCopyMode = False
    With Selection
        .HorizontalAlignment = xlCenter
        .VerticalAlignment = xlBottom
        .WrapText = False
        .Orientation = 0
        .AddIndent = False
        .IndentLevel = 0
        .ShrinkToFit = False
        .ReadingOrder = xlContext
        .MergeCells = False
    End With
    Selection.Merge
    ActiveCell.FormulaR1C1 = "Maior que 15 segundos"
    Range("K2").Select
    Columns("K:K").EntireColumn.AutoFit
    Columns("L:L").EntireColumn.AutoFit
    Columns("M:M").EntireColumn.AutoFit
    Columns("N:N").EntireColumn.AutoFit
    Sheets("Dados Originais").Select
    ActiveWindow.SmallScroll Down:=-138
    ActiveSheet.Range("$A$1:$C$182").AutoFilter Field:=3
    Range("A2").Select
    ActiveWindow.SmallScroll Down:=3
    Sheets.Add After:=ActiveSheet
    Sheets("Plan2").Select
    Sheets("Plan2").Name = "Dict"
    Sheets("Dict").Select
    Sheets("Dict").Move After:=Sheets(3)
    Sheets("Dados Originais").Select
    ActiveWindow.SmallScroll Down:=-27
    Range(Selection, Selection.End(xlDown)).Select
    Selection.Copy
    Sheets("Dict").Select
    Range("A2").Select
    ActiveSheet.Paste
    Range("A1").Select
    Application.CutCopyMode = False
    ActiveCell.FormulaR1C1 = "PMU"
    Range("A1").Select
    Range(Selection, Selection.End(xlDown)).Select
    ActiveSheet.Range("$A$1:$A$182").RemoveDuplicates Columns:=1, Header:=xlNo
    Range("B1").Select
    ActiveCell.FormulaR1C1 = "Cód. PMU"
    Range("C1").Select
    ActiveCell.FormulaR1C1 = "Timestamp"
    Range("C2").Select
    Columns("B:B").EntireColumn.AutoFit
    Columns("C:C").EntireColumn.AutoFit
    Range("C2").Select
    ActiveCell.FormulaR1C1 = "4/2/2014 0:00"
    Range("C2").Select
    Selection.Copy
    Range("C3:C11").Select
    ActiveSheet.Paste
    Range("B2").Select
    Application.CutCopyMode = False
    ActiveCell.FormulaR1C1 = "1"
    Range("B3").Select
    ActiveCell.FormulaR1C1 = "2"
    Range("B2:B3").Select
    Selection.AutoFill Destination:=Range("B2:B11"), Type:=xlFillDefault
    Range("B2:B11").Select
    Sheets("Grupos de Severidade").Select
    Range("B2").Select
    Windows("20140204_20141127_04_5.csv").Activate
    Range("B2").Select
    ActiveCell.FormulaR1C1 = "=VLOOKUP(RC[1],Dict!R2C1:R11C2,2,FALSE)"
    Range("B2").Select
    Selection.Copy
    Range("A2").Select
    Selection.End(xlDown).Select
    Range("B39").Select
    Range(Selection, Selection.End(xlUp)).Select
    Range("B3:B39").Select
    Range("B39").Activate
    ActiveSheet.Paste
    Range("G2").Select
    Application.CutCopyMode = False
    Range("G2").Select
    ActiveCell.FormulaR1C1 = "=VLOOKUP(RC[1],Dict!R2C1:R11C2,2,FALSE)"
    Range("G2").Select
    Selection.Copy
    Range("G3").Select
    Range(Selection, Selection.End(xlDown)).Select
    Range(Selection, Selection.End(xlUp)).Select
    Range("F3").Select
    Selection.End(xlDown).Select
    Range("G28").Select
    Range(Selection, Selection.End(xlUp)).Select
    Range("G3:G28").Select
    Range("G28").Activate
    ActiveSheet.Paste
    Application.CutCopyMode = False
    Selection.End(xlUp).Select
    Range("L2").Select
    ActiveCell.FormulaR1C1 = "=VLOOKUP(RC[1],Dict!R2C1:R11C2,2,FALSE)"
    Range("L2").Select
    Selection.Copy
    Range("K2").Select
    Selection.End(xlDown).Select
    Range("L117").Select
    Range(Selection, Selection.End(xlUp)).Select
    Range("L3:L117").Select
    Range("L117").Activate
    ActiveSheet.Paste
    Selection.End(xlUp).Select
    Range("K1:N1").Select
    Columns("B:B").EntireColumn.AutoFit
    Columns("G:G").EntireColumn.AutoFit
    Columns("L:L").EntireColumn.AutoFit
    Range("A2:B2").Select
    Range(Selection, Selection.End(xlDown)).Select
    ActiveSheet.Shapes.AddChart2(240, xlXYScatter).Select
    ActiveChart.SetSourceData Source:=Range("'Grupos de Severidade'!$A$2:$B$39")
    ActiveChart.ChartTitle.Select
    ActiveChart.ChartTitle.Text = "02/04/2014"
    Selection.Format.TextFrame2.TextRange.Characters.Text = "02/04/2014"
    With Selection.Format.TextFrame2.TextRange.Characters(1, 10).ParagraphFormat
        .TextDirection = msoTextDirectionLeftToRight
        .Alignment = msoAlignCenter
    End With
    With Selection.Format.TextFrame2.TextRange.Characters(1, 10).Font
        .BaselineOffset = 0
        .Bold = msoFalse
        .NameComplexScript = "+mn-cs"
        .NameFarEast = "+mn-ea"
        .Fill.Visible = msoTrue
        .Fill.ForeColor.RGB = RGB(89, 89, 89)
        .Fill.Transparency = 0
        .Fill.Solid
        .Size = 14
        .Italic = msoFalse
        .Kerning = 12
        .Name = "+mn-lt"
        .UnderlineStyle = msoNoUnderline
        .Spacing = 0
        .Strike = msoNoStrike
    End With
    ActiveChart.ChartArea.Select
    ActiveChart.ClearToMatchStyle
    ActiveChart.ChartStyle = 248
    ActiveSheet.Shapes("Gráfico 1").IncrementLeft -89.25
    ActiveSheet.Shapes("Gráfico 1").IncrementTop -192
    ActiveSheet.Shapes("Gráfico 1").ScaleWidth 2.6041666667, msoFalse, _
        msoScaleFromTopLeft
    ActiveSheet.Shapes("Gráfico 1").ScaleHeight 2.7465277778, msoFalse, _
        msoScaleFromTopLeft
    ActiveChart.PlotArea.Select
    Application.CutCopyMode = False
    ActiveChart.SeriesCollection.NewSeries
    ActiveChart.FullSeriesCollection(2).Name = "='Grupos de Severidade'!$F$1:$I$1"
    ActiveChart.FullSeriesCollection(2).XValues = _
        "='Grupos de Severidade'!$F$2:$F$28"
    ActiveChart.FullSeriesCollection(2).Values = _
        "='Grupos de Severidade'!$G$2:$G$28"
    ActiveChart.FullSeriesCollection(1).Name = "='Grupos de Severidade'!$A$1:$D$1"
    ActiveChart.SeriesCollection.NewSeries
    ActiveChart.FullSeriesCollection(3).Name = "='Grupos de Severidade'!$K$1:$N$1"
    ActiveChart.FullSeriesCollection(3).XValues = _
        "='Grupos de Severidade'!$K$2:$K$117"
    ActiveChart.FullSeriesCollection(3).Values = _
        "='Grupos de Severidade'!$L$2:$L$117"
    ActiveWindow.SmallScroll Down:=-96
    ActiveChart.Axes(xlValue).Select
    ActiveChart.Axes(xlValue).MajorUnit = 1
    ActiveChart.FullSeriesCollection(2).Select
    With Selection.Format.Fill
        .Visible = msoTrue
        .ForeColor.ObjectThemeColor = msoThemeColorAccent1
        .ForeColor.TintAndShade = 0
        .ForeColor.Brightness = 0
        .Solid
    End With
    With Selection.Format.Fill
        .Visible = msoTrue
        .ForeColor.RGB = RGB(255, 255, 0)
        .Transparency = 0
        .Solid
    End With
    ActiveChart.FullSeriesCollection(3).Select
    With Selection.Format.Fill
        .Visible = msoTrue
        .ForeColor.RGB = RGB(255, 255, 0)
        .Solid
    End With
    With Selection.Format.Fill
        .Visible = msoTrue
        .ForeColor.RGB = RGB(255, 0, 0)
        .Transparency = 0
        .Solid
    End With
    Application.CommandBars("Format Object").Visible = False
    Sheets("Dict").Select
    Sheets.Add After:=ActiveSheet
    Sheets("Plan3").Select
    Sheets("Plan3").Name = "Gráfico"
    Sheets("Gráfico").Select
    Sheets("Gráfico").Move Before:=Sheets(1)
    Sheets("Grupos de Severidade").Select
    ActiveChart.ChartArea.Select
    ActiveChart.Location Where:=xlLocationAsObject, Name:="Gráfico"
    ActiveSheet.ChartObjects("Gráfico 1").Activate
    ActiveSheet.Shapes("Gráfico 1").IncrementLeft -436.5
    ActiveSheet.Shapes("Gráfico 1").IncrementTop -12.75
    ActiveSheet.ChartObjects("Gráfico 1").Activate
    ActiveSheet.Shapes("Gráfico 1").ScaleWidth 1.4568, msoFalse, _
        msoScaleFromTopLeft
End Sub
