function Get-DataSizeSince {
    <#
    .SYNOPSIS
    Calculates the total size of files in a directory modified since a specific date.
    
    .EXAMPLE
    Get-DataSizeSince -Path "C:\Logs"
    
    .EXAMPLE
    Get-DataSizeSince -Path "D:\Data" -SinceDate "2025-07-01"
    #>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true, Position=0, ValueFromPipeline=$true)]
        [ValidateScript({Test-Path $_ -PathType Container})]
        [string]$Path,

        [Parameter(Mandatory=$false, Position=1)]
        [datetime]$SinceDate = [datetime]'2025-06-01'
    )

    process {
        Write-Host "Scanning '$Path' for files updated since $($SinceDate.ToShortDateString())..." -ForegroundColor Cyan

        # Get all files recursively. ErrorAction suppresses "Access Denied" errors for protected folders.
        $files = Get-ChildItem -Path $Path -Recurse -File -ErrorAction SilentlyContinue | 
                 Where-Object { $_.LastWriteTime -ge $SinceDate }

        # Measure the total size of the filtered files
        $stats = $files | Measure-Object -Property Length -Sum

        # Handle empty results gracefully
        $totalBytes = if ($stats.Sum) { $stats.Sum } else { 0 }
        $fileCount  = if ($stats.Count) { $stats.Count } else { 0 }

        # Output a clean object with formatted sizes
        [PSCustomObject]@{
            TargetDirectory = $Path
            SinceDate       = $SinceDate.ToShortDateString()
            FilesFound      = $fileCount
            SizeInBytes     = $totalBytes
            SizeInMB        = [math]::Round($totalBytes / 1MB, 2)
            SizeInGB        = [math]::Round($totalBytes / 1GB, 2)
        }
    }
}