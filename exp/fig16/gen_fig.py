#! /usr/bin/python3

# Hashmap Latency (Zipf) : Fig 16
import matplotlib.pyplot as plt

# ——— Plot ———
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as ticker
import os


def read_latency_files(workloads, thread_counts, directory="."):
    """
    Read throughput files of the form: res.<workload>.<num_threads>.latency
    
    Args:
        workloads: List of workload names (e.g., ['RDMA', 'Leap', 'Redy', 'PD3'])
        thread_counts: List of thread counts (e.g., [1, 2, 4, 8])
        directory: Directory containing the files (default: current directory)
    
    Returns:
        List of numpy arrays, one per workload, each containing throughput values
        for the corresponding thread counts.
    """
    results = []
    for workload in workloads:
        p50_values = []
        p99_values = []
        for threads in thread_counts:
            filename = f"res.{workload}.{threads}.latency"
            filepath = os.path.join(directory, filename)
            try:
                with open(filepath, 'r') as f:
                    p99, p90, p50 = [float(line.strip()) for line in f]
                    p99_values.append(p99)
                    p50_values.append(p50)
            except FileNotFoundError:
                print(f"Warning: File not found: {filepath}")
                p99_values.append(0.0)
                p50_values.append(0.0)
            except ValueError as e:
                print(f"Warning: Could not parse value in {filepath}: {e}")
                p99_values.append(0.0)
                p50_values.append(0.0)
        results.append(np.array(p99_values))
        results.append(np.array(p50_values))
    return results


# ——— Data ———
thread_counts = [1, 2, 4, 8]
workloads = ['RDMA', 'Leap', 'Redy', 'PD3']

configs = np.array([str(t) for t in thread_counts])

# Read throughput values from files
p99_1, p50_1, p99_2, p50_2, p99_3, p50_3, p99_4, p50_4 = read_latency_files(workloads, thread_counts)
# p99_1, p50_1, p99_2, p50_2, p99_3, p50_3 = read_latency_files(workloads, thread_counts)

uni_fontsize = 10

# colors = ['#76B7B2', '#E15759', '#4E79A7', '#59A14F']
colors = ['#76B7B2', '#E15759', '#F28E2B', '#4E79A7']  # Modern flat colors

# ——— Plot configuration ———
bar_width = 0.2
x = np.arange(len(configs))  # group positions

plt.rcParams['font.family'] = "Liberation Sans"
plt.rcParams['font.size'] = uni_fontsize
plt.rcParams['axes.labelsize'] = uni_fontsize
plt.rcParams['axes.titlesize'] = uni_fontsize
plt.rcParams['xtick.labelsize'] = uni_fontsize
plt.rcParams['ytick.labelsize'] = uni_fontsize
plt.rcParams['legend.fontsize'] = uni_fontsize
plt.rcParams['hatch.linewidth'] = 0.6

fig, ax = plt.subplots(figsize=(3, 2))


# ——— Bars with hatching ———
bars11 = ax.bar(x - bar_width * 1.5, p99_1, bar_width, label='', color=colors[0], hatch='', alpha=0.6)
bars22 = ax.bar(x - bar_width * 0.5, p99_2, bar_width, label='', color=colors[1], hatch='', alpha=0.6)
bars33 = ax.bar(x + bar_width * 0.5, p99_3, bar_width, label='', color=colors[2], hatch='', alpha=0.6)
bars44 = ax.bar(x + bar_width * 1.5, p99_4, bar_width, label='', color=colors[3], hatch='', alpha=0.6)

bars1 = ax.bar(x - bar_width * 1.5, p50_1, bar_width, label='RDMA', color=colors[0], hatch='xxx', edgecolor='black')
bars2 = ax.bar(x - bar_width * 0.5, p50_2, bar_width, label='Leap', color=colors[1], hatch='xx', edgecolor='black')
bars3 = ax.bar(x + bar_width * 0.5, p50_3, bar_width, label='Redy', color=colors[2], hatch='x', edgecolor='black')
bars4 = ax.bar(x + bar_width * 1.5, p50_4, bar_width, label='PD3', color=colors[3], hatch='', edgecolor='black')


ax.set_ylabel('Latency (us)', fontsize=uni_fontsize)
ax.set_xlabel('#Threads', fontsize=uni_fontsize)

# ——— X-axis ———
ax.set_xticks(x)
ax.set_xticklabels(configs)

ax.set_ylim(0, 450)

ax.legend(
    frameon=False,
    loc='upper center',
    ncol=4,
    handlelength=0.6,
    columnspacing=0.5,
    handletextpad=0.3
)
ax.grid(True, which='both', axis='y', linestyle='--', linewidth=0.5)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

ax.spines['left'].set_linewidth(1.5)
ax.spines['bottom'].set_linewidth(1.5)

ax.grid(True, which='major', axis='y', linestyle='--', linewidth=0.6)
ax.grid(False, which='minor', axis='y')  # disable minor grid lines

fig.tight_layout()

# Save figure to the same directory as this script
script_dir = os.path.dirname(os.path.abspath(__file__))
fig.savefig(os.path.join(script_dir, 'fig.png'))