package com.sujoy.trees;

public class GetLeafCountDFS {
	public static void main(String[] args) {
		int count = 0;
		TreeNode root = populatedata();
		count = getLeafCount(root);
		System.out.println("Leaf Count is #" + count);
	}

	public static int getLeafCount(TreeNode root) {
		//System.out.println("Root is :" + root.getData());
		if (root == null)
			return 0;
		if (root.getLeft() == null && root.getRight() == null)
			return 1;
		return getLeafCount(root.getLeft()) + getLeafCount(root.getRight());
	}

	public static TreeNode populatedata() {
		TreeNode root = new TreeNode();
		root.setData(1);

		TreeNode left = new TreeNode();
		root.setLeft(left);
		left.setData(2);

		TreeNode right = new TreeNode();
		root.setRight(right);
		right.setData(3);

		TreeNode leftChild = new TreeNode();
		left.setLeft(leftChild);
		leftChild.setData(4);

		TreeNode rightChild = new TreeNode();
		// right.setRight(rightChild);
		left.setRight(rightChild);
		rightChild.setData(5);

		return root;
	}

}
