"""
WSL Drive Mount Verification Module
OPTIMIZED: Checks drives once, suppresses success logs for individual files.
"""
import subprocess
import logging
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


class WSLDriveMounter:
    """Handles WSL drive mounting verification and mounting"""
    
    def __init__(self):
        self._mounted_cache = set()
    
    def verify_wsl_mount(self, wsl_path: str) -> bool:
        """Verify if a WSL path is accessible (Silent unless error)"""
        try:
            # Use fast check
            cmd = ['wsl', 'test', '-e', wsl_path]
            result = subprocess.run(cmd, capture_output=True, timeout=5)
            return result.returncode == 0
        except Exception:
            return False
    
    def get_mounted_drives(self) -> List[str]:
        """Get list of currently mounted drives in WSL"""
        try:
            cmd = ['wsl', 'ls', '/mnt']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                drives = result.stdout.strip().split('\n')
                return [d.strip() for d in drives if d.strip()]
            return []
        except Exception:
            return []
    
    def mount_drive_windows(self, drive_letter: str) -> bool:
        """Ensure Windows drive is mounted in WSL"""
        drive_letter = drive_letter.lower()
        
        # Cache check to avoid repetitive subprocess calls
        if drive_letter in self._mounted_cache:
            return True
            
        mount_point = f"/mnt/{drive_letter}"
        
        try:
            # Check if already mounted
            mounted = self.get_mounted_drives()
            if drive_letter in mounted:
                self._mounted_cache.add(drive_letter)
                return True
            
            logger.info(f"Mounting drive {drive_letter.upper()} in WSL...")
            cmd = ['wsl', 'ls', mount_point]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                self._mounted_cache.add(drive_letter)
                logger.info(f"✓ Drive {drive_letter.upper()}: Mounted")
                return True
            else:
                logger.error(f"✗ Failed to mount drive {drive_letter.upper()}")
                return False
        except Exception as e:
            logger.error(f"Error mounting drive: {e}")
            return False
    
    def extract_drive_from_path(self, windows_path: str) -> Optional[str]:
        if not windows_path or len(windows_path) < 2: return None
        if windows_path[1] == ':': return windows_path[0].lower()
        return None
    
    def verify_all_tc_paths(self, tc_list: List[dict]) -> dict:
        """
        Verify all TC paths are accessible (QUIET MODE)
        Only prints errors or final summary.
        """
        results = {
            'total': len(tc_list),
            'accessible': 0,
            'inaccessible': []
        }
        
        logger.info("Verifying WSL accessibility...")
        
        # 1. Pre-mount required drives
        required_drives = set()
        for tc in tc_list:
            drive = self.extract_drive_from_path(tc['windows_path'])
            if drive: required_drives.add(drive)
            
        for drive in required_drives:
            self.mount_drive_windows(drive)
            
        # 2. Check files (Silently)
        # Using simple string manipulation is faster than re-importing TCFinder here
        for tc in tc_list:
            drive = self.extract_drive_from_path(tc['windows_path'])
            if not drive: continue
            
            # Fast convert to WSL path manually to avoid circular imports/overhead
            win_path = tc['windows_path']
            wsl_path = win_path.replace('\\', '/').replace(':', '').lower()
            wsl_path = f"/mnt/{drive}/{wsl_path[2:]}"
            
            if self.verify_wsl_mount(wsl_path):
                results['accessible'] += 1
            else:
                results['inaccessible'].append(f"{tc['year']} {tc['storm_name']}")
                logger.warning(f"✗ Inaccessible: {wsl_path}")
        
        # 3. Print Summary
        if len(results['inaccessible']) == 0:
            logger.info(f"✓ All {results['total']} files are accessible in WSL.")
        else:
            logger.warning(f"⚠ Only {results['accessible']}/{results['total']} files accessible.")
            
        return results

def check_wsl_available() -> bool:
    """Check if WSL is available and working"""
    try:
        subprocess.run(['wsl', 'true'], check=True, capture_output=True)
        return True
    except:
        logger.error("WSL not available")
        return False

def diagnose_mount_issues():
    """Diagnostic function to troubleshoot mounting issues (Standalone run only)"""
    print("\n" + "="*70)
    print("WSL MOUNT DIAGNOSTICS")
    print("="*70)
    
    if not check_wsl_available():
        print("✗ WSL is not available")
        return
    
    mounter = WSLDriveMounter()
    drives = mounter.get_mounted_drives()
    print(f"Mounted drives: {drives}")
    
    # Test D: specifically
    print("Testing D: drive...")
    if mounter.mount_drive_windows('d'):
        print("✓ D: drive is accessible")
    else:
        print("✗ D: drive is NOT accessible")
    print("="*70 + "\n")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    diagnose_mount_issues()