import psutil
import matplotlib.pyplot as plt
import datetime as dt

# Get the current CPU usage as a percentage
cpu_percents = []
times = []
# Run for three minutes and create plot
for i in range(240):
    cpu_percent = psutil.cpu_percent()
    cpu_percents.append(cpu_percent)
    times.append(dt.datetime.now())
    plt.pause(1)

# Plot the CPU usage over time
fig, ax = plt.subplots()
ax.plot(times, cpu_percents)
ax.set_ylim([0, 100])
ax.set_xlabel('Time (seconds)')
ax.set_ylabel('CPU Usage (%)')
plt.show()