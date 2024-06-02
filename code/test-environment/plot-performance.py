import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

def plt_1():
    data = pd.read_csv('data/performance1.csv')
    data.columns = [c.strip() for c in data.columns]

    data = data.sort_values('msg_p_s')

    plt.plot(data['msg_p_s'], data['avg_ips'], label='Input Rows per Second', marker='o')
    plt.plot(data['msg_p_s'], data['avg_ops'], label='Output Rows per Second', marker='o')
    plt.xlabel('Messages per Second')
    plt.ylabel('Rows per Second')
    plt.title('Performance Test 1: Varying Producer Speed')
    plt.legend()

    plt.savefig('reports\\performance1.png')

def plt2():
    d = pd.read_csv('data/performance2.csv')
    fig, axs = plt.subplots(3, 1, figsize=(8, 8))

    data = d[d['p_missing'] == 0.0]
    data = data[data['p_extra'] == 0.0]
    data = data.sort_values('p_wrong')
    data.columns = [c.strip() for c in data.columns]
    sns.lineplot(data=data, x='p_wrong', y='avg_ips', label='Input Rows per Second', marker='o', ax=axs[0])
    sns.lineplot(data=data, x='p_wrong', y='avg_ops', label='Processed Rows per Second', marker='o', ax=axs[0])
    axs[0].set_xlabel('Share of measurements with right schema, but non-pollution measurements')
    axs[0].set_ylabel('Rows per Second')
    axs[0].set_title('Varying Wrong Measurements')

    data = d[d['p_wrong'] == 0.0]
    data = data[data['p_extra'] == 0.0]
    data = data.sort_values('p_missing')
    data.columns = [c.strip() for c in data.columns]
    sns.lineplot(data=data, x='p_missing', y='avg_ips', label='Input Rows per Second', marker='o', ax=axs[1])
    sns.lineplot(data=data, x='p_missing', y='avg_ops', label='Processed Rows per Second', marker='o', ax=axs[1])
    axs[1].set_xlabel('Share of measurements with missing values -- wrong schema')
    axs[1].set_ylabel('Rows per Second')
    axs[1].set_title('Varying Missing Values')

    data = d[d['p_missing'] == 0.0]
    data = data[data['p_wrong'] == 0.0]
    data = data.sort_values('p_extra')
    data.columns = [c.strip() for c in data.columns]
    sns.lineplot(data=data, x='p_extra', y='avg_ips', label='Input Rows per Second', marker='o', ax=axs[2])
    sns.lineplot(data=data, x='p_extra', y='avg_ops', label='Processed Rows per Second', marker='o', ax=axs[2])
    axs[2].set_xlabel('Share of measurements with extra fields -- wrong schema')
    axs[2].set_ylabel('Rows per Second')
    axs[2].set_title('Varying Extra Fields')

    plt.tight_layout()
    plt.savefig('reports\\performance2.png')
    plt.show()

data = pd.read_csv('data/performance3.csv')
data.columns = [c.strip() for c in data.columns]

data = data.sort_values('n_sensors')

plt.plot(data['n_sensors'], data['avg_ops'], label='Processed Rows per Second (1 streamapp)', marker='o')
plt.xlabel('Number of Sensors')
plt.ylabel('Processed Rows per Second')
plt.title('Performance Test 3: Varying Number of Sensors + Parallelism')

data = pd.read_csv('data/performance3_2.csv')
data.columns = [c.strip() for c in data.columns]
#plot where runId==01623055-1a3f-4e5b-8ceb-d4f75d65f759
data_a = data[data['runId'] == '01623055-1a3f-4e5b-8ceb-d4f75d65f759']
data_a = data_a.sort_values('n_sensors')
plt.plot(data_a['n_sensors'], data_a['avg_ops'], label='Processed Rows per Second (2 streamapps -- instance b)', marker='o')

data_b = data[data['runId'] == '60cd9a7d-a978-4f0e-8289-229493f4ba49']
data_b = data_b.sort_values('n_sensors')
plt.plot(data_b['n_sensors'], data_b['avg_ops'], label='Processed Rows per Second (2 streamapps -- instance a)', marker='o')

plt.legend()

plt.savefig('reports\\performance3.png')

plt.show()


