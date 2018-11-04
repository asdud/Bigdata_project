package demo.cf;

import java.io.File;
import java.util.List;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;


/*
 * 基本的过程(步骤)
 * 1.分析各个用户（user）对物品（item）的评分
 * 2.根据用户对物品的评分，计算所有用户之间的相似度(余弦相似度、欧式距离等等)
 * 3.选出与当前用户最相似的N个用户
 * 4.将当前用户购买过的商品，推荐给其他用户：反之也行
 */
public class UserBasedCF {
    final static int NEIGHBORHOOD_NUM = 2;
    final static int RECOMMENDER_NUM = 3;

    public static void main(String[] args) throws Exception {
    	//根据数据源建立数据模型，即：打分矩阵
        String file = "D:\\download\\data\\ratingdata.txt";
        DataModel model = new FileDataModel(new File(file));

        //根据打分矩阵,计算用户的相似度
        UserSimilarity usersimilarity = new EuclideanDistanceSimilarity(model);
        
        //找到与用户相近的邻居，即：找到NEIGHBORHOOD_NUM的相邻用户
        //n:找到相似的N个用户
        NearestNUserNeighborhood neighbor = new NearestNUserNeighborhood(NEIGHBORHOOD_NUM, usersimilarity, model);
        
        // 构建基于用户的推荐引擎，其中dataModel为数据模型，neighborhood为用户领域模型，usersimilarity为相似度模型
        Recommender r = new GenericUserBasedRecommender(model, neighbor, usersimilarity);
        
        System.out.println("***************测试一：给一个用户推荐****************");
        //向用户1推荐两个商品  说明：recommender.recommend(userID, howMany)
        List<RecommendedItem> recommendations = r.recommend(1, 2);
		for (RecommendedItem recommendation : recommendations) {
			// 输出推荐结果
			System.out.println("给用户1推荐的商品是：" + recommendation.getItemID());
		}
		
		System.out.println("***************测试二：给每个用户都推荐****************");
		//向每个用户推荐商品
        LongPrimitiveIterator iter = model.getUserIDs();
        while (iter.hasNext()) {
            long uid = iter.nextLong();
            System.out.printf("用户ID:%s  ", uid);            
            
            List<RecommendedItem> list = r.recommend(uid, RECOMMENDER_NUM);
            for (RecommendedItem ritem : list) {
                System.out.printf("(%s,%f)", ritem.getItemID(), ritem.getValue());
            }
            System.out.println();
        }
    }
}