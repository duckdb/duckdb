$urls = Get-Content -Path "files.txt"
foreach ($url in $urls) {
    curl $url -OutFile ".\data\" + [System.IO.Path]::GetFileName($url)
}