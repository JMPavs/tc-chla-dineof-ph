"""
DINEOF Init Manager (Dynamic Edition)
- FIX: Template now accepts ALL parameters dynamically (no hardcoded neini=1)
- Replicates your manual successful run structure exactly.
- Uses separate output files for Chl, SST, U, V.
"""
from pathlib import Path
from typing import Dict, Any
import logging
import re
import xarray as xr

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DINEOFInitManager:
    
    # === CRITICAL UPDATE: ALL VALUES ARE NOW DYNAMIC ===
    TEMPLATE = """! DINEOF 2.0 Input File
! --------------------- INPUT ---------------------!
data = {data_list}
mask = {mask_list}
time = '{input_file}#time'
!---------------- DINEOF PARAMETERS ----------------!
alpha   = {alpha}
numit   = {numit}
nev     = {nev}
neini   = {neini}
ncv     = {ncv}
tol     = {tol}
nitemax = {nitemax}
toliter = {toliter}
rec     = {rec}
eof     = {eof}
norm    = {norm}
!---------------- OUTPUT ----------------!
Output = '{output_dir}'
results = {results_list}
seed = {seed}
cloud_size = {cloud_size}
!---------------- EOF OUTPUT ----------------!
EOF.U   = {eof_u_list}
EOF.V   = '{eof_file}#V'
EOF.Sigma = '{eof_file}#Sigma'
! END OF PARAMETER FILE
"""
    
    def __init__(self):
        pass
    
    def _ensure_wsl_path(self, path_str: str) -> str:
        if path_str.startswith('/mnt/'): return path_str
        p = path_str.replace('\\', '/').replace('"', '').replace("'", "")
        if ':' in p:
            drive = p[0].lower()
            p = f"/mnt/{drive}{p[2:]}"
        return p

    def create_init_file(self, tc_info: Dict, output_wsl_dir: str, 
                        output_file_name: str = "dineof.init",
                        params: Dict[str, Any] = None) -> str:
        
        if not output_wsl_dir.endswith('/'): output_wsl_dir += '/'
        
        input_wsl_path = self._ensure_wsl_path(tc_info['wsl_path'])
        input_win_path = Path(tc_info['windows_path'])
        
        # Prepare Lists
        vars_to_process = []
        
        try:
            with xr.open_dataset(input_win_path) as ds:
                # 1. Chlorophyll
                if 'chlor_a_anom_norm' in ds:
                    vars_to_process.append(('chlor_a_anom_norm', 'dineof_chlor_a.nc#chlor_a'))
                elif 'chlor_a_log10_anom_clim' in ds:
                    vars_to_process.append(('chlor_a_log10_anom_clim', 'dineof_chlor_a.nc#chlor_a'))
                
                # 2. Physics
                if 'sst_anom_norm' in ds:
                    vars_to_process.append(('sst_anom_norm', 'dineof_sst.nc#sst'))
                if 'u_anom_norm' in ds:
                    vars_to_process.append(('u_anom_norm', 'dineof_u_wind.nc#u'))
                if 'v_anom_norm' in ds:
                    vars_to_process.append(('v_anom_norm', 'dineof_v_wind.nc#v'))
                    
            if not vars_to_process:
                logger.warning(f"No known variables found in {input_win_path}, defaulting to basic chl")
                vars_to_process = [('chlor_a_anom_norm', 'dineof_chlor_a.nc#chlor_a')]
                
        except Exception as e:
            logger.error(f"Failed to inspect file: {e}")
            vars_to_process = [('chlor_a_anom_norm', 'dineof_chlor_a.nc#chlor_a')]

        data_entries = []
        mask_entries = []
        results_entries = []
        eof_u_entries = []
        
        eof_file = f"{output_wsl_dir}eof.nc"

        for in_var, out_def in vars_to_process:
            out_file, out_var = out_def.split('#')
            
            # Input
            data_entries.append(f"'{input_wsl_path}#{in_var}'")
            
            # Mask (Repeated)
            mask_entries.append(f"'{input_wsl_path}#mask'")
            
            # Output (Separate Files)
            results_entries.append(f"'{output_wsl_dir}{out_file}#{out_var}'")
            
            # EOF U
            eof_u_entries.append(f"'{output_wsl_dir}eof_{out_file}#U_{out_var}'")

        # Join Lists
        data_str = "[" + ", ".join(data_entries) + "]"
        mask_str = "[" + ", ".join(mask_entries) + "]"
        res_str = "[" + ", ".join(results_entries) + "]"
        eof_u_str = "[" + ", ".join(eof_u_entries) + "]"

        # --- UPDATED DEFAULTS MAPPING ---
        # Default settings if none are provided
        defaults = {
            'alpha': 0.01, 'numit': 3, 'nev': 20, 'neini': 10, 'ncv': 50,
            'tol': 1.0e-8, 'nitemax': 500, 'toliter': 1.0e-3, 
            'rec': 1, 'eof': 1, 'norm': 1, 'seed': 243435, 'cloud_size': 1000
        }
        
        # Override defaults with passed params (from Config)
        if params: 
            defaults.update(params)
            
        # Safety check: Krylov subspace size vs Modes
        if defaults['ncv'] < defaults['nev'] + 5: 
            defaults['ncv'] = defaults['nev'] + 5

        replacements = {
            'data_list': data_str,
            'mask_list': mask_str,
            'results_list': res_str,
            'eof_u_list': eof_u_str,
            'eof_file': eof_file,
            'input_file': input_wsl_path,
            'output_dir': output_wsl_dir,
            **defaults  # Unpacks all the numeric parameters dynamically
        }
        
        init_content = self.TEMPLATE.format(**replacements)
        
        win_out_dir_str = output_wsl_dir.replace('/mnt/d', 'D:').replace('/', '\\')
        output_file_path = Path(win_out_dir_str) / output_file_name
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file_path, 'w', newline='\n') as f:
            f.write(init_content)
            
        return str(output_file_path)
    
    def validate_init_file(self, init_file_path: str) -> bool:
        try:
            with open(init_file_path, 'r') as f:
                content = f.read()
            return 'EOF.U' in content and 'mask' in content
        except: return False