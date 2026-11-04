package testutil

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// RequireLargeTrial checks explicit opt-in and free space on the volume that
// actually backs t.TempDir(). If the probe tool is unavailable the opt-in
// trial skips instead of failing.
func RequireLargeTrial(t testing.TB, minFree int64) {
	t.Helper()
	if os.Getenv("ARTIFACT_SERVICES_LARGE_TEST") != "1" {
		t.Skip("opt in with ARTIFACT_SERVICES_LARGE_TEST=1")
	}
	cmd := exec.Command("powershell.exe", "-NoProfile", "-Command", "[System.IO.DriveInfo]::new([System.IO.Path]::GetPathRoot($env:ARTIFACT_SERVICES_PROBE_DIR)).AvailableFreeSpace")
	cmd.Env = append(os.Environ(), "ARTIFACT_SERVICES_PROBE_DIR="+t.TempDir())
	out, err := cmd.Output()
	if err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			t.Skip("powershell.exe unavailable for free-space probe")
		}
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
