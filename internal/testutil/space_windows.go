package testutil

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// RequireLargeTrial checks explicit opt-in and temp-volume free space.
func RequireLargeTrial(t testing.TB, minFree int64) {
	t.Helper()
	if os.Getenv("ARTIFACT_SERVICES_LARGE_TEST") != "1" {
		t.Skip("opt in with ARTIFACT_SERVICES_LARGE_TEST=1")
	}
	out, err := exec.Command("powershell.exe", "-NoProfile", "-Command", "[System.IO.DriveInfo]::new([System.IO.Path]::GetPathRoot([System.IO.Path]::GetTempPath())).AvailableFreeSpace").Output()
	if err != nil {
		t.Fatal(err)
	}
	free, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if free < minFree {
		t.Fatalf("need %d free; available=%d", minFree, free)
	}
	t.Logf("free_before=%d", free)
}
