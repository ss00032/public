# Excel COM 객체 생성
$excel = New-Object -ComObject Excel.Application
$excel.visible=$false
$excel.displayalerts = $false
# 엑셀 파일이 있는 디렉토리
$directory = "C:\Users\Owner\Documents\source\검수된매핑정의서"

# 디렉토리 내 모든 엑셀 파일을 반복 처리
Get-ChildItem -Path $directory -Filter *.xlsx | ForEach-Object {
    $currentName = $_.Name  # 현재 파일 이름 저장
    $newName = $currentName -replace "_V.0.6", "_V.1.0"  # 새로운 파일 이름 생성
    Rename-Item -Path $_.FullName -NewName $newName  # 파일 이름 변경
}

Get-ChildItem -Path $directory -Filter *.xlsx | ForEach-Object {
    $excelFilePath = $_.FullName
    $excelFilePath
    $workbook = $excel.Workbooks.Open($excelFilePath)


    # 대상 엑셀 파일에서 첫 번째 시트 삭제
    $targetWorksheet = $workbook.Sheets.Item("개정이력")
    $targetWorksheet.Cells.Clear()
    # 모든 도형 삭제
    $targetWorksheet.Shapes | ForEach-Object {
        $_.Delete()
    }
    $targetWorksheet.Delete()

    # 대상 엑셀 파일에서 첫 번째 시트 삭제
    $targetWorksheet = $workbook.Sheets.Item("표지")
    $targetWorksheet.Cells.Clear()
    # 모든 도형 삭제
    $targetWorksheet.Shapes | ForEach-Object {
        $_.Delete()
    }
    $targetWorksheet.Delete()

    <## 템플릿 엑셀 파일 열기
    $templateWorkbook = $excel.Workbooks.Open("C:\Users\Owner\Documents\source\templeate.xlsx")

    # 템플릿 워크시트를 복사하여 대상 엑셀 파일에 붙여넣기
    $templateWorksheet = $templateWorkbook.Sheets.Item("표지")  # 템플릿 워크시트 선택 (이름 변경 필요)
    $templateWorksheet.Copy()
    $targetWorksheet = $excel.ActiveSheet  # 대상 워크시트로 지정#>

    # 템플릿 엑셀 파일 열기
    $templateWorkbook = $excel.Workbooks.Open("C:\Users\Owner\Documents\source\templeate.xlsx")

    # 템플릿 엑셀 파일에서 표지 시트를 복사하여 대상 엑셀 파일에 붙여넣기
    $templateWorksheet = $templateWorkbook.Sheets.Item("개정이력")  # "표지 시트 이름"은 실제 템플릿 엑셀 파일의 표지 시트 이름으로 바꿔야 합니다.
    $templateWorksheet.Copy($workbook.Sheets.Item(1))

    $templateWorksheet = $templateWorkbook.Sheets.Item("표지")  # "표지 시트 이름"은 실제 템플릿 엑셀 파일의 표지 시트 이름으로 바꿔야 합니다.
    $templateWorksheet.Copy($workbook.Sheets.Item(1))

    # 수정된 내용을 저장하고 파일을 닫습니다.
    $workbook.Save()
    $workbook.Close()
    $templateWorkbook.Close()
}

# Excel COM 객체를 종료합니다.
$excel.Quit()
$ExcelObjSub.Quit()
[System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel)
