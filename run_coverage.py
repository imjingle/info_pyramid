#!/usr/bin/env python3
"""
Test coverage report generator for AK Unified.
Generates comprehensive coverage reports and analysis.
"""

import subprocess
import sys
import os
from pathlib import Path


def run_command(command, description):
    """Run a command and return success status."""
    print(f"\n🔧 {description}")
    print(f"Running: {command}")
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} completed successfully")
            if result.stdout:
                print("Output:", result.stdout.strip())
            return True
        else:
            print(f"❌ {description} failed")
            if result.stderr:
                print("Error:", result.stderr.strip())
            return False
    except Exception as e:
        print(f"❌ {description} failed with exception: {e}")
        return False


def generate_coverage_report():
    """Generate comprehensive test coverage report."""
    print("📊 Generating Test Coverage Report for AK Unified")
    print("=" * 60)
    
    # Check if we're in the right directory
    if not Path("src/ak_unified").exists():
        print("❌ Error: src/ak_unified directory not found. Please run from project root.")
        return False
    
    # Install coverage dependencies if needed
    print("\n📦 Installing coverage dependencies...")
    install_commands = [
        "uv pip install pytest-cov",
        "uv pip install coverage[toml]"
    ]
    
    for cmd in install_commands:
        if not run_command(cmd, f"Installing {cmd.split()[-1]}"):
            print("⚠️  Some dependencies may not be installed. Continuing...")
    
    # Run tests with coverage
    print("\n🧪 Running tests with coverage...")
    coverage_command = (
        "python -m pytest tests/ "
        "--cov=src/ak_unified "
        "--cov-report=term-missing "
        "--cov-report=html:htmlcov "
        "--cov-report=xml:coverage.xml "
        "--cov-fail-under=80 "
        "-v"
    )
    
    if not run_command(coverage_command, "Running tests with coverage"):
        print("⚠️  Tests failed, but coverage data may still be available")
    
    # Generate detailed coverage report
    print("\n📈 Generating detailed coverage report...")
    detailed_coverage = "python -m coverage report --show-missing"
    
    if not run_command(detailed_coverage, "Detailed coverage report"):
        print("⚠️  Detailed coverage report failed")
    
    # Generate HTML coverage report
    print("\n🌐 Generating HTML coverage report...")
    html_coverage = "python -m coverage html --title='AK Unified Coverage Report'"
    
    if not run_command(html_coverage, "HTML coverage report"):
        print("⚠️  HTML coverage report failed")
    
    # Check coverage files
    coverage_files = [
        "htmlcov/index.html",
        "coverage.xml",
        ".coverage"
    ]
    
    print("\n📁 Checking coverage files...")
    for file_path in coverage_files:
        if Path(file_path).exists():
            print(f"✅ {file_path} generated")
        else:
            print(f"❌ {file_path} not found")
    
    # Generate coverage summary
    print("\n📊 Coverage Summary:")
    print("-" * 40)
    
    try:
        # Try to read coverage data
        if Path(".coverage").exists():
            result = subprocess.run(
                "python -m coverage report --format=total",
                shell=True, capture_output=True, text=True
            )
            if result.returncode == 0:
                print(result.stdout.strip())
            else:
                print("Could not generate coverage summary")
        else:
            print("No coverage data found")
    except Exception as e:
        print(f"Error generating coverage summary: {e}")
    
    return True


def analyze_coverage():
    """Analyze coverage data and provide insights."""
    print("\n🔍 Analyzing Coverage Data...")
    print("=" * 40)
    
    # Check if coverage data exists
    if not Path(".coverage").exists():
        print("❌ No coverage data found. Run tests with coverage first.")
        return False
    
    try:
        # Get coverage summary
        result = subprocess.run(
            "python -m coverage report --show-missing",
            shell=True, capture_output=True, text=True
        )
        
        if result.returncode == 0:
            print("📊 Coverage Report:")
            print(result.stdout.strip())
            
            # Analyze missing lines
            missing_lines = []
            for line in result.stdout.split('\n'):
                if 'Missing' in line and 'Missing:' not in line:
                    parts = line.split()
                    if len(parts) >= 4:
                        file_path = parts[0]
                        missing_count = parts[-1]
                        if missing_count.isdigit() and int(missing_count) > 0:
                            missing_lines.append((file_path, int(missing_count)))
            
            if missing_lines:
                print(f"\n⚠️  Files with missing coverage:")
                for file_path, missing_count in sorted(missing_lines, key=lambda x: x[1], reverse=True):
                    print(f"  {file_path}: {missing_count} lines missing")
            
            # Check coverage threshold
            try:
                threshold_result = subprocess.run(
                    "python -m coverage report --format=total",
                    shell=True, capture_output=True, text=True
                )
                if threshold_result.returncode == 0:
                    total_line = threshold_result.stdout.strip()
                    if "TOTAL" in total_line:
                        parts = total_line.split()
                        if len(parts) >= 4:
                            coverage_percent = parts[-1].rstrip('%')
                            try:
                                coverage_value = float(coverage_percent)
                                if coverage_value >= 80:
                                    print(f"\n🎉 Coverage target achieved: {coverage_value:.1f}% >= 80%")
                                else:
                                    print(f"\n⚠️  Coverage below target: {coverage_value:.1f}% < 80%")
                            except ValueError:
                                print(f"\n⚠️  Could not parse coverage percentage: {coverage_percent}")
            except Exception as e:
                print(f"Error checking coverage threshold: {e}")
        
        else:
            print("❌ Failed to generate coverage report")
            print("Error:", result.stderr.strip())
            
    except Exception as e:
        print(f"Error analyzing coverage: {e}")
        return False
    
    return True


def generate_coverage_badge():
    """Generate coverage badge for README."""
    print("\n🏷️  Generating Coverage Badge...")
    
    try:
        if Path(".coverage").exists():
            result = subprocess.run(
                "python -m coverage report --format=total",
                shell=True, capture_output=True, text=True
            )
            
            if result.returncode == 0:
                total_line = result.stdout.strip()
                if "TOTAL" in total_line:
                    parts = total_line.split()
                    if len(parts) >= 4:
                        coverage_percent = parts[-1].rstrip('%')
                        try:
                            coverage_value = float(coverage_percent)
                            
                            # Determine badge color
                            if coverage_value >= 90:
                                color = "brightgreen"
                            elif coverage_value >= 80:
                                color = "green"
                            elif coverage_value >= 70:
                                color = "yellow"
                            elif coverage_value >= 60:
                                color = "orange"
                            else:
                                color = "red"
                            
                            badge_url = f"https://img.shields.io/badge/coverage-{coverage_value:.1f}%25-{color}"
                            print(f"Coverage Badge URL: {badge_url}")
                            
                            # Save badge info to file
                            with open("coverage_badge.txt", "w") as f:
                                f.write(f"Coverage: {coverage_value:.1f}%\n")
                                f.write(f"Badge URL: {badge_url}\n")
                            
                            print("✅ Coverage badge info saved to coverage_badge.txt")
                            
                        except ValueError:
                            print("⚠️  Could not parse coverage percentage")
                    else:
                        print("⚠️  Unexpected coverage report format")
                else:
                    print("⚠️  No TOTAL line found in coverage report")
            else:
                print("❌ Failed to get coverage data")
                
    except Exception as e:
        print(f"Error generating coverage badge: {e}")
        return False
    
    return True


def main():
    """Main function to run coverage analysis."""
    print("🚀 AK Unified Test Coverage Analysis")
    print("=" * 50)
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ required")
        return 1
    
    # Generate coverage report
    if not generate_coverage_report():
        print("❌ Failed to generate coverage report")
        return 1
    
    # Analyze coverage
    if not analyze_coverage():
        print("⚠️  Coverage analysis failed")
    
    # Generate badge
    if not generate_coverage_badge():
        print("⚠️  Badge generation failed")
    
    print("\n🎯 Coverage Analysis Complete!")
    print("\n📁 Generated Files:")
    print("  - htmlcov/index.html (HTML coverage report)")
    print("  - coverage.xml (XML coverage report)")
    print("  - .coverage (Coverage data)")
    print("  - coverage_badge.txt (Badge information)")
    
    print("\n🌐 View HTML Report:")
    print("  Open htmlcov/index.html in your browser")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())