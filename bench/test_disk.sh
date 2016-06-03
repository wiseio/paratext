# Tests a filesystem's warm and cold performance over time. Run the directory where the device is mounted.
# Throttled disks should result in variable results. Otherwise, the results should be closer to constant.

device="$1"
log="$2"

for i in $(seq 1 1000);
do
  disk_results="$(sudo bash -c "hdparm -Tt ${device} | grep Timing | sed -e 's/.*=//g' | sed -e 's/ MB\/sec//g'")"
  disk_A="$(echo $disk_results | cut -d' ' -f1)"
  disk_B="$(echo $disk_results | cut -d' ' -f2)"
  sudo bash -c "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"
  run_experiment.py - cmd=countnl disk_state="cold" filename="floats.csv" diskA="$disk_A" diskB="$disk_B" log="$log" num_threads=32
  run_experiment.py - cmd=countnl disk_state="warm" filename="floats.csv" log="$log" num_threads=32
done

