# Hashmap Throughput (Uniform) : Fig 11
import matplotlib.pyplot as plt

# ——— Plot ———
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as ticker
import os


def read_throughput_files(workloads, thread_counts, directory="."):
    """
    Read throughput files of the form: res.<workload>.<num_threads>.tput
    
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
        values = []
        for threads in thread_counts:
            filename = f"res.{workload}.{threads}.tput"
            filepath = os.path.join(directory, filename)
            try:
                with open(filepath, 'r') as f:
                    value = float(f.read().strip())
                    values.append(value)
            except FileNotFoundError:
                print(f"Warning: File not found: {filepath}")
                values.append(0.0)
            except ValueError as e:
                print(f"Warning: Could not parse value in {filepath}: {e}")
                values.append(0.0)
        results.append(np.array(values))
    return results


# ——— Data ———
thread_counts = [1, 2, 4, 8]
workloads = ['RDMA', 'Leap', 'Redy', 'PD3']

configs = np.array([str(t) for t in thread_counts])

# Read throughput values from files
values_op1, values_op2, values_op3, values_op4 = read_throughput_files(workloads, thread_counts)

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
bars1 = ax.bar(x - bar_width * 1.5, values_op1, bar_width, label='RDMA', color=colors[0], hatch='xxx', edgecolor='black')
bars2 = ax.bar(x - bar_width * 0.5, values_op2, bar_width, label='Leap', color=colors[1], hatch='xx', edgecolor='black')
bars3 = ax.bar(x + bar_width * 0.5, values_op3, bar_width, label='Redy', color=colors[2], hatch='x', edgecolor='black')
bars4 = ax.bar(x + bar_width * 1.5, values_op4, bar_width, label='PD3', color=colors[3], hatch='', edgecolor='black')


ax.set_ylabel('Throughput (M ops/s)', fontsize=uni_fontsize)
ax.set_xlabel('#Threads', fontsize=uni_fontsize)

# ——— X-axis ———
ax.set_xticks(x)
ax.set_xticklabels(configs)

ax.set_ylim(0, 70)

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
fig.savefig(os.path.join(script_dir, 'fig.pdf'))
