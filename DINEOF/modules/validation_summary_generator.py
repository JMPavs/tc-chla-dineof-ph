"""
Validation Summary Generator
Consolidates validation results from all TCs into a single master CSV
"""
import pandas as pd
from pathlib import Path
import logging
from typing import List

logger = logging.getLogger(__name__)


class ValidationSummaryGenerator:
    """Generates master validation summary CSV"""
    
    def __init__(self, output_base_dir: Path):
        """
        Initialize summary generator
        
        Args:
            output_base_dir: Base output directory containing TC folders
        """
        self.output_base_dir = Path(output_base_dir)
    
    def generate_summary(self, save_filename: str = "validation_summary_all_TCs.csv") -> pd.DataFrame:
        """
        Scan all TC output folders and consolidate validation results
        
        Args:
            save_filename: Name of summary CSV file
            
        Returns:
            DataFrame with validation summary for all TCs
        """
        logger.info("Generating validation summary...")
        logger.info(f"Scanning: {self.output_base_dir}")
        
        all_validations = []
        
        # Find all validation_metrics_*.csv files
        validation_files = list(self.output_base_dir.glob("Output_*/validation_metrics_*.csv"))
        
        logger.info(f"Found {len(validation_files)} validation files")
        
        for val_file in validation_files:
            try:
                df = pd.read_csv(val_file)
                
                # Extract essential columns
                if len(df) > 0:
                    row = df.iloc[0].to_dict()
                    
                    # Create summary record
                    summary = {
                        'tc_name': row.get('tc_name', 'Unknown'),
                        'year': row.get('year', None),
                        'storm_name': row.get('storm_name', 'Unknown'),
                        'validation_status': row.get('validation_status', 'UNKNOWN'),
                        'flags_passed': row.get('flags_passed', 0),
                        'flags_total': row.get('flags_total', 0),
                        
                        # Key metrics
                        'correlation': row.get('correlation', None),
                        'r_squared': row.get('r_squared', None),
                        'rmse_linear': row.get('rmse_linear', None),
                        'bias': row.get('bias', None),
                        'relative_bias_pct': row.get('relative_bias_pct', None),
                        'mape': row.get('mape', None),
                        
                        # CV metrics
                        'optimal_nev': row.get('optimal_nev', None),
                        'min_cv_error': row.get('min_cv_error', None),
                        
                        # Gap filling
                        'gaps_filled_pct': row.get('gaps_filled_pct', None),
                        'missing_data_pct': row.get('missing_data_pct', None),
                        
                        # Individual flags
                        'flag_cv_optimal_found': row.get('flag_cv_optimal_found', None),
                        'flag_cv_error_acceptable': row.get('flag_cv_error_acceptable', None),
                        'flag_high_correlation': row.get('flag_high_correlation', None),
                        'flag_low_bias': row.get('flag_low_bias', None),
                        'flag_good_rmse': row.get('flag_good_rmse', None),
                        'flag_gaps_filled': row.get('flag_gaps_filled', None),
                        
                        # Processing info
                        'processing_time_sec': row.get('processing_time_sec', None),
                        'n_valid_points': row.get('n_valid_points', None),
                        
                        # File reference
                        'validation_file': val_file.name,
                        'output_folder': val_file.parent.name
                    }
                    
                    all_validations.append(summary)
                    
            except Exception as e:
                logger.warning(f"Failed to read {val_file}: {e}")
                continue
        
        if not all_validations:
            logger.warning("No validation results found!")
            return pd.DataFrame()
        
        # Create DataFrame
        df_summary = pd.DataFrame(all_validations)
        
        # Sort by year and storm name
        df_summary = df_summary.sort_values(['year', 'storm_name']).reset_index(drop=True)
        
        # Save to CSV
        summary_file = self.output_base_dir / save_filename
        df_summary.to_csv(summary_file, index=False)
        logger.info(f"✓ Saved validation summary: {summary_file}")
        
        # Print summary statistics
        self._print_summary_stats(df_summary)
        
        return df_summary
    
    def _print_summary_stats(self, df: pd.DataFrame):
        """Print summary statistics"""
        print("\n" + "="*70)
        print("VALIDATION SUMMARY - ALL TCs")
        print("="*70)
        
        total = len(df)
        passed = (df['validation_status'] == 'PASS').sum()
        warning = (df['validation_status'] == 'WARNING').sum()
        
        print(f"\nTotal TCs Validated: {total}")
        print(f"  ✓ PASS: {passed} ({passed/total*100:.1f}%)")
        print(f"  ⚠ WARNING: {warning} ({warning/total*100:.1f}%)")
        
        if 'year' in df.columns and df['year'].notna().any():
            year_range = f"{int(df['year'].min())}-{int(df['year'].max())}"
            print(f"  Year Range: {year_range}")
        
        # Flag statistics
        print("\nQuality Flag Success Rates:")
        flag_cols = [col for col in df.columns if col.startswith('flag_')]
        for flag_col in flag_cols:
            if flag_col in df.columns and df[flag_col].notna().any():
                flag_name = flag_col.replace('flag_', '').replace('_', ' ').title()
                pass_rate = df[flag_col].sum() / df[flag_col].notna().sum() * 100
                print(f"  {flag_name}: {pass_rate:.1f}%")
        
        # Key metrics
        if 'correlation' in df.columns and df['correlation'].notna().any():
            print(f"\nKey Metrics (mean ± std):")
            print(f"  Correlation: {df['correlation'].mean():.3f} ± {df['correlation'].std():.3f}")
            print(f"  RMSE: {df['rmse_linear'].mean():.3f} ± {df['rmse_linear'].std():.3f} mg/m³")
            print(f"  Bias: {df['bias'].mean():.3f} ± {df['bias'].std():.3f} mg/m³")
            print(f"  Gaps Filled: {df['gaps_filled_pct'].mean():.1f}% ± {df['gaps_filled_pct'].std():.1f}%")
        
        # TCs with warnings
        if warning > 0:
            print(f"\nTCs with WARNINGS ({warning}):")
            warning_tcs = df[df['validation_status'] == 'WARNING'][['tc_name', 'flags_passed', 'flags_total']]
            for idx, row in warning_tcs.iterrows():
                print(f"  - {row['tc_name']}: {row['flags_passed']}/{row['flags_total']} flags passed")
        
        print("="*70 + "\n")
    
    def create_filtered_summaries(self, df: pd.DataFrame):
        """Create additional filtered summary files"""
        
        # 1. PASS only
        df_pass = df[df['validation_status'] == 'PASS']
        if len(df_pass) > 0:
            pass_file = self.output_base_dir / "validation_summary_PASS_only.csv"
            df_pass.to_csv(pass_file, index=False)
            logger.info(f"✓ Saved PASS-only summary: {pass_file}")
        
        # 2. WARNING only
        df_warning = df[df['validation_status'] == 'WARNING']
        if len(df_warning) > 0:
            warning_file = self.output_base_dir / "validation_summary_WARNING_only.csv"
            df_warning.to_csv(warning_file, index=False)
            logger.info(f"⚠ Saved WARNING-only summary: {warning_file}")
        
        # 3. By year
        if 'year' in df.columns and df['year'].notna().any():
            for year in sorted(df['year'].dropna().unique()):
                df_year = df[df['year'] == year]
                year_file = self.output_base_dir / f"validation_summary_{int(year)}.csv"
                df_year.to_csv(year_file, index=False)
                logger.info(f"✓ Saved {int(year)} summary: {year_file}")


def generate_validation_summary(output_base_dir: str):
    """
    Convenience function to generate validation summary
    
    Args:
        output_base_dir: Path to base output directory
        
    Usage:
        generate_validation_summary(r"D:\Thesis\Output_Anomalous")
    """
    generator = ValidationSummaryGenerator(output_base_dir)
    df_summary = generator.generate_summary()
    
    if len(df_summary) > 0:
        generator.create_filtered_summaries(df_summary)
        
        print("\n✓ Validation summary generation complete!")
        print(f"  Main file: validation_summary_all_TCs.csv")
        print(f"  Filtered files: *_PASS_only.csv, *_WARNING_only.csv")
        print(f"  Location: {output_base_dir}")
    else:
        print("\n⚠ No validation results found to summarize")
    
    return df_summary


# Example usage
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # Generate summary for all TCs
    output_dir = r"D:\Thesis_2\Output_Normal_3"
    df = generate_validation_summary(output_dir)
    
    # Show TCs ready for analysis
    if df is not None and len(df) > 0:
        print("\n" + "="*70)
        print("TCs READY FOR ANALYSIS (PASS status)")
        print("="*70)
        
        df_pass = df[df['validation_status'] == 'PASS']
        if len(df_pass) > 0:
            for idx, row in df_pass.iterrows():
                print(f"  {row['tc_name']}: r²={row['r_squared']:.3f}, RMSE={row['rmse_linear']:.3f}")
        else:
            print("  No TCs passed all validation checks")
        
        print("="*70 + "\n")