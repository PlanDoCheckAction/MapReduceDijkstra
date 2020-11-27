import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class DijkstraReducer extends Reducer<Text, Node, Text, Text> {

    protected void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {

        //到点Key的最短路径及其前驱节点
        Integer minDistance = Integer.MAX_VALUE;
        Integer minPreX = Integer.MAX_VALUE;
        Integer minPreY = Integer.MAX_VALUE;

        int i = 0;
        Node V = null;

        for (Node node : values) {
            if (node.isNode()) {
                V = new Node(key.toString(), node.toString());
                continue;
            }
            if (minDistance > node.getDistance()){
                minDistance = node.getDistance();
                minPreX = node.getPreNodeX();
                minPreY = node.getPreNodeY();
            }
        }

        if (V.getDistance() > minDistance){
            V.setDistance(minDistance);
            V.setPreNodeX(minPreX);
            V.setPreNodeY(minPreY);
            context.getCounter(eInf.COUNTER).increment(1L);
        }

        context.write(key, new Text(V.toString()));
    }
}
