package org.apache.hadoop.hdfs.server.datanode;

public class MyDataSet {
	 private long cpuFrequence;//CPUƵ��
     private int cpuCore;//CPU����
     private long mem;//�ڴ��С
     private long net;//����������
     private long disk;//�����̴�������
     private long capacity;//�ڵ�Ĵ洢�ռ��С
     private long dfsUsed;//�ڵ��hdfsʹ�õĿռ��С
     private long capacityUsed;//��Ⱥ�����нڵ�ʹ�õ��ܵĿռ��С
     

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
