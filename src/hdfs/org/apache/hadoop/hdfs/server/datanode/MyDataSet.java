package org.apache.hadoop.hdfs.server.datanode;

public class MyDataSet {
	 private long cpuFrequence;//CPU频率
     private int cpuCore;//CPU核数
     private long mem;//内存大小
     private long net;//最大网络带宽
     private long disk;//最大磁盘传输速率
     private long capacity;//节点的存储空间大小
     private long dfsUsed;//节点的hdfs使用的空间大小
     private long capacityUsed;//集群中所有节点使用的总的空间大小
     

     public long getCapacityUsed() {
		return capacityUsed;
	}


	public void setCapacityUsed(long capacityUsed) {
		this.capacityUsed = capacityUsed;
	}


	public long getCapacity() {
		return capacity;
	}


	public void setCapacity(long capacity) {
		this.capacity = capacity;
	}


	public long getDfsUsed() {
		return dfsUsed;
	}


	public void setDfsUsed(long dfsUsed) {
		this.dfsUsed = dfsUsed;
	}


	public long getCpuFrequence() {
		return cpuFrequence;
	}


	public void setCpuFrequence(long cpuFrequence) {
		this.cpuFrequence = cpuFrequence;
	}


	public int getCpuCore() {
		return cpuCore;
	}


	public void setCpuCore(int cpuCore) {
		this.cpuCore = cpuCore;
	}


	public long getMem() {
		return mem;
	}


	public void setMem(long mem) {
		this.mem = mem;
	}


	public long getNet() {
		return net;
	}


	public void setNet(long net) {
		this.net = net;
	}


	public long getDisk() {
		return disk;
	}


	public void setDisk(long disk) {
		this.disk = disk;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
