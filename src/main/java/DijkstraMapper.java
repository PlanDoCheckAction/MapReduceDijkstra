

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.List;

public class DijkstraMapper extends Mapper<Text, Text, Text, Node> {

    Text K = new Text();

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        int conuter = context.getConfiguration().getInt("run.counter", 1);

        Node node = new Node(key.toString(), value.toString());
        System.out.println("Key:" + key.toString());
        System.out.println("Value:" + value.toString());

        // 第一次计算
        if (conuter == 1) {

            String[] startXY = StringUtils.split(key.toString(), ' ');

            int startX = Integer.parseInt(startXY[0]);
            int startY = Integer.parseInt(startXY[1]);

            //如果是起始点，则设起始点的权值为0
            if (startX == Constant.startX && startY == Constant.startY) {
                node.setDistance(0);
            }
        }
        //key输出格式：X Y
        //value输出格式：Distance preNodeX preNodeY|X Y Wight preNodeX preNodeY|X Y Wight preNodeX preNodeY
        context.write(key, node);

        // 没走到此节点 退出
        if (Integer.MAX_VALUE == node.getDistance())
            return;

        // 如果有邻接节点，将邻接节点输出
        List<Node> adjs = node.getAdjs();
        for (Node adj:adjs) {
            String k = adj.getX() + " " + adj.getY();
            adj.setDistance(adj.getDistance() + node.getDistance());
            K.set(k);
            //key输出格式：X Y
            //value输出格式：Wight preNodeX preNodeY
            context.write(K, adj);
        }
    }
}
