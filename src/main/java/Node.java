import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//格式：Distance preNodeX preNodeY|X Y Wight preNodeX preNodeY|X Y Wight preNodeX preNodeY
public class Node implements Writable {
    private Integer X;
    private Integer Y;
    private Integer distance;
    private Integer preNodeX;
    private Integer preNodeY;
    private boolean isNode = false;
    private List<Node> adjs;
    private Integer adjsLen = 0;

    public Node(String thisXY, String str) {
        if (str.length() == 0)
            return;

        String[] strs = StringUtils.split(str, '|');

        String[] thisNode = StringUtils.split(strs[0], ' ');
        isNode = true;
        distance = Integer.parseInt(thisNode[0]);
        preNodeX = Integer.parseInt(thisNode[1]);
        preNodeY = Integer.parseInt(thisNode[2]);

        adjsLen = strs.length-1;

        adjs = new ArrayList<Node>();

        //邻接节点的前驱节点，即它本身
        String[] XY = StringUtils.split(thisXY, ' ');

        for (int i = 0; i<adjsLen; i++){

            String[] adj = StringUtils.split(strs[i+1], ' ');

            //添加邻接节点
            adjs.add(new Node(adj[0], adj[1], adj[2], XY[0], XY[1]));
        }

    }

    public Node() {

    }

    public Node(String X, String Y, String distance, String preX, String preY) {
        this.X = Integer.parseInt(X);
        this.Y = Integer.parseInt(Y);
        this.distance = Integer.parseInt(distance);
        this.preNodeX = Integer.parseInt(preX);
        this.preNodeY = Integer.parseInt(preY);
    }

    public Integer getAdjsLen() {
        return adjsLen;
    }

    public Integer getX() {
        return X;
    }

    public Integer getY() {
        return Y;
    }

    public Integer getDistance() {
        return distance;
    }

    public Integer getPreNodeX() {
        return preNodeX;
    }

    public Integer getPreNodeY() {
        return preNodeY;
    }

    public boolean isNode() {
        return isNode;
    }

    public void setX(Integer x) {
        X = x;
    }

    public void setY(Integer y) {
        Y = y;
    }

    public void setDistance(Integer distance) {
        this.distance = distance;
    }

    public void setPreNodeX(Integer preNodeX) {
        this.preNodeX = preNodeX;
    }

    public void setPreNodeY(Integer preNodeY) {
        this.preNodeY = preNodeY;
    }

    public void setNode(boolean node) {
        isNode = node;
    }

    public List<Node> getAdjs() {
        return adjs;
    }

    public void setAdjs(List<Node> adjs) {
        this.adjs = adjs;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(distance);
        out.writeInt(preNodeX);
        out.writeInt(preNodeY);
        out.writeBoolean(isNode);
        if (isNode){
            out.writeInt(adjsLen);
            for (Node node:adjs) {
                out.writeInt(node.getX());
                out.writeInt(node.getY());
                out.writeInt(node.getDistance());
                out.writeInt(node.getPreNodeX());
                out.writeInt(node.getPreNodeY());
            }
        }
    }

    public void readFields(DataInput in) throws IOException {
        distance = in.readInt();
        preNodeX = in.readInt();
        preNodeY = in.readInt();
        isNode = in.readBoolean();
        if (isNode){
            adjsLen = in.readInt();
            adjs = new ArrayList<Node>();
            for (int i = 0; i < adjsLen; i++){
                Node temp = new Node();
                temp.setX(in.readInt());
                temp.setY(in.readInt());
                temp.setDistance(in.readInt());
                temp.setPreNodeX(in.readInt());
                temp.setPreNodeY(in.readInt());
                adjs.add(temp);
            }
        }
    }

    @Override
    public String toString() {

        StringBuffer result = new StringBuffer(distance + " " + preNodeX + " " + preNodeY);

        if (isNode) {
            for (Node node : adjs) {
                result.append("|" + node.X + " " + node.Y + " " + node.distance + " " + node.preNodeX
                        + " " + node.preNodeY);
            }
        }

        return result.toString();
    }

}
