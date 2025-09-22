"""
Simple monitoring dashboard for the distributed queue

This provides a terminal-based dashboard showing real-time metrics.
"""

import time
import os
import sys
import requests
from datetime import datetime
from typing import Dict, Any

def clear_screen():
    """Clear the terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def format_number(num: float, decimals: int = 2) -> str:
    """Format a number with thousand separators"""
    if num < 1000:
        return f"{num:.{decimals}f}"
    elif num < 1000000:
        return f"{num/1000:.1f}K"
    else:
        return f"{num/1000000:.1f}M"

def format_duration(seconds: float) -> str:
    """Format duration in human-readable format"""
    if seconds < 1:
        return f"{seconds*1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"

def print_header():
    """Print dashboard header"""
    print("=" * 80)
    print("                    DISTRIBUTED QUEUE MONITORING DASHBOARD")
    print("=" * 80)

def print_section(title: str):
    """Print section separator"""
    print(f"\n{'-' * 30} {title} {'-' * 30}")

def display_metrics(metrics: Dict[str, Any]):
    """Display metrics in a formatted dashboard"""
    clear_screen()
    print_header()
    
    # Timestamp and uptime
    print(f"\n📅 Last Update: {metrics['timestamp']}")
    print(f"⏱️  Uptime: {format_duration(metrics['uptime_seconds'])}")
    
    # Queue Status
    print_section("QUEUE STATUS")
    gauges = metrics['gauges']
    print(f"  📊 Queue Depth:     {gauges['queue_depth']:,}")
    print(f"  👷 Active Workers:  {gauges['active_workers']}")
    print(f"  ⚙️  Active Tasks:    {gauges['active_tasks']}")
    
    # Task Counters
    print_section("TASK COUNTERS")
    counters = metrics['counters']
    print(f"  ✅ Completed:       {counters['tasks_completed']:,}")
    print(f"  ❌ Failed:          {counters['tasks_failed']:,}")
    print(f"  🔄 Retried:         {counters['tasks_retried']:,}")
    print(f"  💀 Dead Lettered:   {counters['tasks_dead_lettered']:,}")
    print(f"  📥 Total Submitted: {counters['tasks_submitted']:,}")
    
    # Performance Metrics
    print_section("PERFORMANCE")
    rates = metrics['rates']
    print(f"  📈 Submission Rate: {rates['submission_rate']:.2f} tasks/sec")
    print(f"  📊 Throughput:      {rates['throughput_rate']:.2f} tasks/sec")
    print(f"  ✨ Success Rate:    {rates['success_rate']:.1f}%")
    
    # Processing Times
    if metrics.get('processing_times'):
        print_section("PROCESSING TIMES")
        pt = metrics['processing_times']
        print(f"  ⚡ Average:         {format_duration(pt['avg'])}")
        print(f"  🚀 P50 (Median):    {format_duration(pt['p50'])}")
        print(f"  📊 P95:             {format_duration(pt['p95'])}")
        print(f"  🔥 P99:             {format_duration(pt['p99'])}")
        print(f"  📉 Min:             {format_duration(pt['min'])}")
        print(f"  📈 Max:             {format_duration(pt['max'])}")
    
    # Wait Times
    if metrics.get('wait_times'):
        print_section("WAIT TIMES")
        wt = metrics['wait_times']
        print(f"  ⏳ Average:         {format_duration(wt['avg'])}")
        print(f"  📊 P95:             {format_duration(wt['p95'])}")
    
    # Task Distribution
    if metrics.get('tasks_by_type'):
        print_section("TASK DISTRIBUTION")
        for task_type, count in metrics['tasks_by_type'].items():
            failures = metrics.get('failures_by_type', {}).get(task_type, 0)
            failure_rate = (failures / count * 100) if count > 0 else 0
            print(f"  📦 {task_type:15} {count:6,} tasks  ({failure_rate:.1f}% failure rate)")
    
    # Recent Errors
    if metrics.get('recent_errors'):
        print_section("RECENT ERRORS (Last 5)")
        for error in metrics['recent_errors'][-5:]:
            error_time = datetime.fromtimestamp(error['timestamp']).strftime('%H:%M:%S')
            error_msg = error['error'][:60] + '...' if len(error['error']) > 60 else error['error']
            print(f"  [{error_time}] {error['task_type']}: {error_msg}")
    
    print("\n" + "=" * 80)
    print("Press Ctrl+C to exit")

def display_health(health: Dict[str, Any]):
    """Display health status in a simple format"""
    clear_screen()
    print_header()
    
    status_emoji = "✅" if health['status'] == 'healthy' else "⚠️"
    print(f"\n{status_emoji} System Status: {health['status'].upper()}")
    print(f"📅 Checked: {health['timestamp']}")
    print(f"⏱️  Uptime: {format_duration(health['uptime_seconds'])}")
    
    print_section("HEALTH CHECKS")
    checks = health['checks']
    for check_name, status in checks.items():
        emoji = "✅" if status == 'ok' else ("⚠️" if status == 'warning' else "❌")
        print(f"  {emoji} {check_name.replace('_', ' ').title():20} {status.upper()}")
    
    if health.get('warnings'):
        print_section("WARNINGS")
        for warning in health['warnings']:
            print(f"  ⚠️  {warning}")
    
    print_section("METRICS SUMMARY")
    summary = health['metrics_summary']
    for key, value in summary.items():
        print(f"  {key.replace('_', ' ').title():20} {value}")
    
    print("\n" + "=" * 80)

def main(api_url: str = "http://localhost:8000", refresh_interval: int = 2):
    """
    Run the monitoring dashboard
    
    Args:
        api_url: Base URL of the API server
        refresh_interval: Seconds between updates
    """
    print("Starting Distributed Queue Monitoring Dashboard...")
    print(f"Connecting to {api_url}...")
    
    mode = "metrics"  # Can be 'metrics' or 'health'
    
    try:
        while True:
            try:
                if mode == "metrics":
                    response = requests.get(f"{api_url}/api/v1/metrics", timeout=2)
                    if response.status_code == 200:
                        metrics = response.json()
                        display_metrics(metrics)
                    else:
                        print(f"Error fetching metrics: {response.status_code}")
                else:
                    response = requests.get(f"{api_url}/api/v1/health", timeout=2)
                    if response.status_code == 200:
                        health = response.json()
                        display_health(health)
                    else:
                        print(f"Error fetching health: {response.status_code}")
                
                time.sleep(refresh_interval)
                
            except requests.RequestException as e:
                clear_screen()
                print_header()
                print(f"\n❌ Error connecting to API: {e}")
                print(f"\nMake sure the API server is running at {api_url}")
                print("Retrying in 5 seconds...")
                time.sleep(5)
                
    except KeyboardInterrupt:
        clear_screen()
        print("\n👋 Dashboard stopped by user")
        sys.exit(0)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Distributed Queue Monitoring Dashboard")
    parser.add_argument("--url", default="http://localhost:8000", 
                       help="API server URL (default: http://localhost:8000)")
    parser.add_argument("--refresh", type=int, default=2,
                       help="Refresh interval in seconds (default: 2)")
    parser.add_argument("--mode", choices=["metrics", "health"], default="metrics",
                       help="Display mode: metrics or health (default: metrics)")
    
    args = parser.parse_args()
    
    if args.mode == "health":
        # For health mode, use longer refresh interval
        args.refresh = max(args.refresh, 5)
    
    main(args.url, args.refresh)