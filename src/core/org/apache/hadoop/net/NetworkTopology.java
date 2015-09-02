/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.net;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.record.Index;

/** The class represents a cluster of computer with a tree hierarchical
 * network topology.
 * For example, a cluster may be consists of many data centers filled 
 * with racks of computers.
 * In a network topology, leaves represent data nodes (computers) and inner
 * nodes represent switches/routers that manage traffic in/out of data centers
 * or racks.  
 * 
 */
public class NetworkTopology {
  public final static String DEFAULT_RACK = "/default-rack";
  public final static int DEFAULT_HOST_LEVEL = 2;
  public static final Log LOG = 
    LogFactory.getLog(NetworkTopology.class);
    
  /**Inner Node represent a switch/router of a data center or rack.
   * Different from a leave node, it has non-null children.
   */
  private class InnerNode extends NodeBase {
    private ArrayList<Node> children=new ArrayList<Node>();
    private int numOfLeaves;
   
        
    /** Construct an InnerNode from a path-like string */
    InnerNode(String path) {
      super(path);
    }
        
    /** Construct an InnerNode from its name and its network location */
    InnerNode(String name, String location) {
      super(name, location);
    }
        
    /** Construct an InnerNode
     * from its name, its network location, its parent, and its level */
    InnerNode(String name, String location, InnerNode parent, int level) {
      super(name, location, parent, level);
    }
        
    /** Get its children */
    Collection<Node> getChildren() {return children;}
        
    /** Return the number of children this node has */
    int getNumOfChildren() {
      return children.size();
    }
        
    /** Judge if this node represents a rack 
     * Return true if it has no child or its children are not InnerNodes
     */ 
    boolean isRack() {
      if (children.isEmpty()) {
        return true;
      }
            
      Node firstChild = children.get(0);
      if (firstChild instanceof InnerNode) {
        return false;
      }
            
      return true;
    }
        
    /** Judge if this node is an ancestor of node <i>n</i>
     * 
     * @param n a node
     * @return true if this node is an ancestor of <i>n</i>
     */
    boolean isAncestor(Node n) {
      return getPath(this).equals(NodeBase.PATH_SEPARATOR_STR) ||
        (n.getNetworkLocation()+NodeBase.PATH_SEPARATOR_STR).
        startsWith(getPath(this)+NodeBase.PATH_SEPARATOR_STR);
    }
        
    /** Judge if this node is the parent of node <i>n</i>
     * 
     * @param n a node
     * @return true if this node is the parent of <i>n</i>
     */
    boolean isParent(Node n) {
      return n.getNetworkLocation().equals(getPath(this));
    }
        
    /* Return a child name of this node who is an ancestor of node <i>n</i> */
    private String getNextAncestorName(Node n) {
      if (!isAncestor(n)) {
        throw new IllegalArgumentException(
                                           this + "is not an ancestor of " + n);
      }
      String name = n.getNetworkLocation().substring(getPath(this).length());
      if (name.charAt(0) == PATH_SEPARATOR) {
        name = name.substring(1);
      }
      int index=name.indexOf(PATH_SEPARATOR);
      if (index !=-1)
        name = name.substring(0, index);
      return name;
    }
        
    /** Add node <i>n</i> to the subtree of this node 
     * @param n node to be added
     * @return true if the node is added; false otherwise
     */
    boolean add(Node n) {
      if (!isAncestor(n))
        throw new IllegalArgumentException(n.getName()+", which is located at "
                +n.getNetworkLocation()+", is not a decendent of "
                +getPath(this));
      if (isParent(n)) {
        // this node is the parent of n; add n directly
        n.setParent(this);
        n.setLevel(this.level+1);
        for(int i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(n.getName())) {
            children.set(i, n);
            return false;
          }
        }
        children.add(n);
        numOfLeaves++;
        return true;
      } else {
        // find the next ancestor node
        String parentName = getNextAncestorName(n);
        InnerNode parentNode = null;
        for(int i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(parentName)) {
            parentNode = (InnerNode)children.get(i);
            break;
          }
        }
        if (parentNode == null) {
          // create a new InnerNode
          parentNode = new InnerNode(parentName, getPath(this),
                                     this, this.getLevel()+1);
          children.add(parentNode);
        }
        // add n to the subtree of the next ancestor node
        if (parentNode.add(n)) {
          numOfLeaves++;
          return true;
        } else {
          return false;
        }
      }
    }
        
    /** Remove node <i>n</i> from the subtree of this node
     * @param n node to be deleted 
     * @return true if the node is deleted; false otherwise
     */
    boolean remove(Node n) {
      String parent = n.getNetworkLocation();
      String currentPath = getPath(this);
      if (!isAncestor(n))
        throw new IllegalArgumentException(n.getName()
                                           +", which is located at "
                                           +parent+", is not a descendent of "+currentPath);
      if (isParent(n)) {
        // this node is the parent of n; remove n directly
        for(int i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(n.getName())) {
            children.remove(i);
            numOfLeaves--;
            n.setParent(null);
            return true;
          }
        }
        return false;
      } else {
        // find the next ancestor node: the parent node
        String parentName = getNextAncestorName(n);
        InnerNode parentNode = null;
        int i;
        for(i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(parentName)) {
            parentNode = (InnerNode)children.get(i);
            break;
          }
        }
        if (parentNode==null) {
          return false;
        }
        // remove n from the parent node
        boolean isRemoved = parentNode.remove(n);
        // if the parent node has no children, remove the parent node too
        if (isRemoved) {
          if (parentNode.getNumOfChildren() == 0) {
            children.remove(i);
          }
          numOfLeaves--;
        }
        return isRemoved;
      }
    } // end of remove
        
    /** Given a node's string representation, return a reference to the node */ 
    private Node getLoc(String loc) {
      if (loc == null || loc.length() == 0) return this;
            
      String[] path = loc.split(PATH_SEPARATOR_STR, 2);
      Node childnode = null;
      for(int i=0; i<children.size(); i++) {
        if (children.get(i).getName().equals(path[0])) {
          childnode = children.get(i);
        }
      }
      if (childnode == null) return null; // non-existing node
      if (path.length == 1) return childnode;
      if (childnode instanceof InnerNode) {
        return ((InnerNode)childnode).getLoc(path[1]);
      } else {
        return null;
      }
    }
        
    /** get <i>leafIndex</i> leaf of this subtree 
     * if it is not in the <i>excludedNode</i>*/
    private Node getLeaf(int leafIndex, Node excludedNode) {
      int count=0;
      // check if the excluded node a leaf
      boolean isLeaf =
        excludedNode == null || !(excludedNode instanceof InnerNode);
      // calculate the total number of excluded leaf nodes
      int numOfExcludedLeaves =
        isLeaf ? 1 : ((InnerNode)excludedNode).getNumOfLeaves();
      if (isRack()) { // children are leaves
        if (isLeaf) { // excluded node is a leaf node
          int excludedIndex = children.indexOf(excludedNode);
          if (excludedIndex != -1 && leafIndex >= 0) {
            // excluded node is one of the children so adjust the leaf index
            leafIndex = leafIndex>=excludedIndex ? leafIndex+1 : leafIndex;
          }
        }
        // range check
        if (leafIndex<0 || leafIndex>=this.getNumOfChildren()) {
          return null;
        }
        return children.get(leafIndex);
      } else {
        for(int i=0; i<children.size(); i++) {
          InnerNode child = (InnerNode)children.get(i);
          if (excludedNode == null || excludedNode != child) {
            // not the excludedNode
            int numOfLeaves = child.getNumOfLeaves();
            if (excludedNode != null && child.isAncestor(excludedNode)) {
              numOfLeaves -= numOfExcludedLeaves;
            }
            if (count+numOfLeaves > leafIndex) {
              // the leaf is in the child subtree
              return child.getLeaf(leafIndex-count, excludedNode);
            } else {
              // go to the next child
              count = count+numOfLeaves;
            }
          } else { // it is the excluededNode
            // skip it and set the excludedNode to be null
            excludedNode = null;
          }
        }
        return null;
      }
    }
        
    int getNumOfLeaves() {
      return numOfLeaves;
    }
  } // end of InnerNode
    
  InnerNode clusterMap = new InnerNode(InnerNode.ROOT); // the root
  private int numOfRacks = 0;  // rack counter
  private ReadWriteLock netlock;
    
  public NetworkTopology() {
    netlock = new ReentrantReadWriteLock();
  }
    
  /** Add a leaf node
   * Update node counter & rack counter if neccessary
   * @param node
   *          node to be added
   * @exception IllegalArgumentException if add a node to a leave 
                                         or node to be added is not a leaf
   */
  public void add(Node node) {
    if (node==null) return;
    if( node instanceof InnerNode ) {
      throw new IllegalArgumentException(
        "Not allow to add an inner node: "+NodeBase.getPath(node));
    }
    netlock.writeLock().lock();
    try {
      Node rack = getNode(node.getNetworkLocation());
      if (rack != null && !(rack instanceof InnerNode)) {
        throw new IllegalArgumentException("Unexpected data node " 
                                           + node.toString() 
                                           + " at an illegal network location");
      }
      if (clusterMap.add(node)) {
        LOG.info("Adding a new node: "+NodeBase.getPath(node));
        if (rack == null) {
          numOfRacks++;
        }
      }
      LOG.debug("NetworkTopology became:\n" + this.toString());
    } finally {
      netlock.writeLock().unlock();
    }
  }
    
  /** Remove a node
   * Update node counter & rack counter if neccessary
   * @param node
   *          node to be removed
   */ 
  public void remove(Node node) {
    if (node==null) return;
    if( node instanceof InnerNode ) {
      throw new IllegalArgumentException(
        "Not allow to remove an inner node: "+NodeBase.getPath(node));
    }
    LOG.info("Removing a node: "+NodeBase.getPath(node));
    netlock.writeLock().lock();
    try {
      if (clusterMap.remove(node)) {
        InnerNode rack = (InnerNode)getNode(node.getNetworkLocation());
        if (rack == null) {
          numOfRacks--;
        }
      }
      LOG.debug("NetworkTopology became:\n" + this.toString());
    } finally {
      netlock.writeLock().unlock();
    }
  }
       
  /** Check if the tree contains node <i>node</i>
   * 
   * @param node
   *          a node
   * @return true if <i>node</i> is already in the tree; false otherwise
   */
  public boolean contains(Node node) {
    if (node == null) return false;
    netlock.readLock().lock();
    try {
      Node parent = node.getParent();
      for(int level=node.getLevel(); parent!=null&&level>0;
          parent=parent.getParent(), level--) {
        if (parent == clusterMap)
          return true;
      }
    } finally {
      netlock.readLock().unlock();
    }
    return false; 
  }
    
  /** Given a string representation of a node, return its reference
   * 
   * @param loc
   *          a path-like string representation of a node
   * @return a reference to the node; null if the node is not in the tree
   */
  public Node getNode(String loc) {
    netlock.readLock().lock();
    try {
      loc = NodeBase.normalize(loc);
      if (!NodeBase.ROOT.equals(loc))
        loc = loc.substring(1);
      return clusterMap.getLoc(loc);
    } finally {
      netlock.readLock().unlock();
    }
  }
    
  /** Return the total number of racks */
  public int getNumOfRacks() {
    netlock.readLock().lock();
    try {
      return numOfRacks;
    } finally {
      netlock.readLock().unlock();
    }
  }
    
  /** Return the total number of nodes */
  public int getNumOfLeaves() {
    netlock.readLock().lock();
    try {
      return clusterMap.getNumOfLeaves();
    } finally {
      netlock.readLock().unlock();
    }
  }
    
  /** Return the distance between two nodes
   * It is assumed that the distance from one node to its parent is 1
   * The distance between two nodes is calculated by summing up their distances
   * to their closest common  ancestor.
   * @param node1 one node
   * @param node2 another node
   * @return the distance between node1 and node2
   * node1 or node2 do not belong to the cluster
   */
  public int getDistance(Node node1, Node node2) {
    if (node1 == node2) {
      return 0;
    }
    Node n1=node1, n2=node2;
    int dis = 0;
    netlock.readLock().lock();
    try {
      int level1=node1.getLevel(), level2=node2.getLevel();
      while(n1!=null && level1>level2) {
        n1 = n1.getParent();
        level1--;
        dis++;
      }
      while(n2!=null && level2>level1) {
        n2 = n2.getParent();
        level2--;
        dis++;
      }
      while(n1!=null && n2!=null && n1.getParent()!=n2.getParent()) {
        n1=n1.getParent();
        n2=n2.getParent();
        dis+=2;
      }
    } finally {
      netlock.readLock().unlock();
    }
    if (n1==null) {
      LOG.warn("The cluster does not contain node: "+NodeBase.getPath(node1));
      return Integer.MAX_VALUE;
    }
    if (n2==null) {
      LOG.warn("The cluster does not contain node: "+NodeBase.getPath(node2));
      return Integer.MAX_VALUE;
    }
    return dis+2;
  } 
    
  /** Check if two nodes are on the same rack
   * @param node1 one node
   * @param node2 another node
   * @return true if node1 and node2 are pm the same rack; false otherwise
   * @exception IllegalArgumentException when either node1 or node2 is null, or
   * node1 or node2 do not belong to the cluster
   */
  public boolean isOnSameRack( Node node1,  Node node2) {
    if (node1 == null || node2 == null) {
      return false;
    }
      
    netlock.readLock().lock();
    try {
      return node1.getParent()==node2.getParent();
    } finally {
      netlock.readLock().unlock();
    }
  }
    
  final private static Random r = new Random();
  /** randomly choose one node from <i>scope</i>
   * if scope starts with ~, choose one from the all nodes except for the
   * ones in <i>scope</i>; otherwise, choose one from <i>scope</i>
   * @param scope range of nodes from which a node will be choosen
   * 随机选择一个数据节点
   * @return the choosen node
   */
  public Node chooseRandom(String scope) {
    netlock.readLock().lock();
    try {
      if (scope.startsWith("~")) {
        return chooseRandom(NodeBase.ROOT, scope.substring(1));
      } else {
        return chooseRandom(scope, null);
      }
    } finally {
      netlock.readLock().unlock();
    }
  }
    
  private Node chooseRandom(String scope, String excludedScope){
	//如果排除范围不为空
    if (excludedScope != null) {
    //如果排除范围和查找范围一致
      if (scope.startsWith(excludedScope)) {
        return null;
      }
      //如果排除范围和查找范围不一致
      if (!excludedScope.startsWith(scope)) {
        excludedScope = null;
      }
    }
    //获取该范围下面所有的节点，包括路由器和交换机节点
    Node node = getNode(scope);
    //如果该node不是路由器则可以直接返回了，因为只有一个选择
    if (!(node instanceof InnerNode)) {
      return node;
    }
    /**
     * Node接口，定义了网络节点的一些方法，NodeBase是Node的一个实现，
     * 提供了叶子节点的一些方法（明显它没有子节点），而InnerNode则实现了
     * 树的内部节点，如果我们考虑一个网络部署的话，那么叶子节点是服务器，
     * 而InnerNode则是服务器所在的机架或交换机或路由器。Node提供了对网络
     * 位置信息（采用类似文件树的方式），节点名称和Node所在的树的深度的方法。
     * NodeBase提供了一个简单的实现。InnerNode是NetworkTopology的内部类，
     * 对比NodeBase，它的clildren保存了所有的子节点，这样的话，就可以构造
     * 一个拓扑树。这棵树的叶子可能是服务器，也可能是机架，内部则是机架或者是
     * 路由器等设备，InnerNode提供了一系列的方法区分处理这些信息。
     NetworkTopology的add方法和remove用于在拓扑结构中加入节点和删除节点，同时也
       给出一些get*方法，用于获取一些对象内部的信息，如getDistance，可以获取两个节
点的距离，而isOnSameRack可以判断两个节点是否处于同一个机架。chooseRandom有
两个实现，用于在一定范围内（另一个还有一个排除选项）随机选取一个节点。
chooseRandom在选择数据块副本位置的时候调用。
DNSToSwitchMapping配合上面NetworkTopology，用于确定某一个节点的网络位置
信息，它的唯一方法，可以通过一系列机器的名字找出它们对应的网络位置信息。目前有支持两种方法，一是通过命令行方式，将节点名作为输入，输出为网络位置信息（RawScriptBasedMapping执行命令CachedDNSToSwitchMapping缓存结果），还有一种就是利用配置参数hadoop.configured.node.mapping静态配置（StaticMapping）。
     */
    //如果是路由器或者交换机的时候，则需要搜索其下面的叶子节点
    InnerNode innerNode = (InnerNode)node;
    //获取该innderNode下有多少叶子节点
    int numOfDatanodes = innerNode.getNumOfLeaves();
    //在这里重用了node变量，将其作为了excludeScope
    if (excludedScope == null) {
      node = null;
    } else {
      //获取排除的节点node
      node = getNode(excludedScope);
      //如果该node范围就是一个节点，则排除一个节点，因此numOfDatanodes数会减1
      if (!(node instanceof InnerNode)) {
        numOfDatanodes -= 1;
      } else {
    	//如果不是一个节点，则会排除一个范围，这样numOfDatanodes会减去该范围下的所有节点的数目
        numOfDatanodes -= ((InnerNode)node).getNumOfLeaves();
      }
    }
    //此处可以加入代码进行最优副本放置策略的选择*********************************/////////////////
    //扫描在可选择范围内的nodes
    Map<String, Double> map = NameNode.dataClassfi;
    List<IndexAndClassfi> classfiList = new ArrayList<IndexAndClassfi>();
    IndexAndClassfi indexAndClassfi;
    for (int i = 0; i < numOfDatanodes; i++) {
		String nodeName = innerNode.getLeaf(i, node).getName();
		indexAndClassfi = new IndexAndClassfi();
		indexAndClassfi.setIndex(i);
		indexAndClassfi.setClassfi(map.get(node).doubleValue());
		if (classfiList.size() ==0) {
			classfiList.add(indexAndClassfi);
		}else {
			int flag =0;
			for (int j = 0; j < classfiList.size(); j++) {
				if (indexAndClassfi.getClassfi()> classfiList.get(j).getClassfi()) {
					flag++;
					continue;
				}else
				break;
				
			}
			//将元素放到指定的位置
			if(flag == classfiList.size())
			{
				classfiList.add(indexAndClassfi);
			}else {
				classfiList.add(flag,indexAndClassfi);
			}
			
		}
		
		
	}
    //***************************************************************************///////////
    /*int leaveIndex = r.nextInt(numOfDatanodes);
    return innerNode.getLeaf(leaveIndex, node);*/
    //返回的是保存在classfiList中的第一个元素
    return innerNode.getLeaf(classfiList.get(0).getIndex(), node);
  }
     
  /** return the number of leaves in <i>scope</i> but not in <i>excludedNodes</i>
   * if scope starts with ~, return the number of nodes that are not
   * in <i>scope</i> and <i>excludedNodes</i>; 
   * @param scope a path string that may start with ~
   * @param excludedNodes a list of nodes
   * @return number of available nodes
   */
  public int countNumOfAvailableNodes(String scope,
                                      List<Node> excludedNodes) {
    boolean isExcluded=false;
    if (scope.startsWith("~")) {
      isExcluded=true;
      scope=scope.substring(1);
    }
    scope = NodeBase.normalize(scope);
    int count=0; // the number of nodes in both scope & excludedNodes
    netlock.readLock().lock();
    try {
      for(Node node:excludedNodes) {
        if ((NodeBase.getPath(node)+NodeBase.PATH_SEPARATOR_STR).
            startsWith(scope+NodeBase.PATH_SEPARATOR_STR)) {
          count++;
        }
      }
      Node n=getNode(scope);
      int scopeNodeCount=1;
      if (n instanceof InnerNode) {
        scopeNodeCount=((InnerNode)n).getNumOfLeaves();
      }
      if (isExcluded) {
        return clusterMap.getNumOfLeaves()-
          scopeNodeCount-excludedNodes.size()+count;
      } else {
        return scopeNodeCount-count;
      }
    } finally {
      netlock.readLock().unlock();
    }
  }
    
  /** convert a network tree to a string */
  public String toString() {
    // print the number of racks
    StringBuffer tree = new StringBuffer();
    tree.append("Number of racks: ");
    tree.append(numOfRacks);
    tree.append("\n");
    // print the number of leaves
    int numOfLeaves = getNumOfLeaves();
    tree.append("Expected number of leaves:");
    tree.append(numOfLeaves);
    tree.append("\n");
    // print nodes
    for(int i=0; i<numOfLeaves; i++) {
      tree.append(NodeBase.getPath(clusterMap.getLeaf(i, null)));
      tree.append("\n");
    }
    return tree.toString();
  }

  /* swap two array items */
  static private void swap(Node[] nodes, int i, int j) {
    Node tempNode;
    tempNode = nodes[j];
    nodes[j] = nodes[i];
    nodes[i] = tempNode;
    
  }
  
  /** Sort nodes array by their distances to <i>reader</i>
   * It linearly scans the array, if a local node is found, swap it with
   * the first element of the array.
   * If a local rack node is found, swap it with the first element following
   * the local node.
   * If neither local node or local rack node is found, put a random replica
   * location at postion 0.
   * It leaves the rest nodes untouched.
   */
  public void pseudoSortByDistance( Node reader, Node[] nodes ) {
    int tempIndex = 0;
    if (reader != null ) {
      int localRackNode = -1;
      //scan the array to find the local node & local rack node
      for(int i=0; i<nodes.length; i++) {
        if(tempIndex == 0 && reader == nodes[i]) { //local node
          //swap the local node and the node at position 0
          if( i != 0 ) {
            swap(nodes, tempIndex, i);
          }
          tempIndex=1;
          if(localRackNode != -1 ) {
            if(localRackNode == 0) {
              localRackNode = i;
            }
            break;
          }
        } else if(localRackNode == -1 && isOnSameRack(reader, nodes[i])) {
          //local rack
          localRackNode = i;
          if(tempIndex != 0 ) break;
        }
      }

      // swap the local rack node and the node at position tempIndex
      if(localRackNode != -1 && localRackNode != tempIndex ) {
        swap(nodes, tempIndex, localRackNode);
        tempIndex++;
      }
    }
    
    // put a random node at position 0 if it is not a local/local-rack node
    if(tempIndex == 0 && nodes.length != 0) {
      swap(nodes, 0, r.nextInt(nodes.length));
    }
  }
}
