package demo.cf;


import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveArrayIterator;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;


/*
 * 
 */
public class ItemBasedCF {
	public static void main(String[] args) throws IOException, TasteException {
		//读入(用户-物品-评分)数据，建立打分矩阵
		 String file = "D:\\download\\data\\ratingdata.txt";
	     DataModel dataModel = new FileDataModel(new File(file));
	     
	     //计算物品的相似度矩阵
	     ItemSimilarity itemSimilarity=new  EuclideanDistanceSimilarity(dataModel);
	     Recommender r=new GenericItemBasedRecommender(dataModel, itemSimilarity);
	     
	     LongPrimitiveIterator iter=dataModel.getUserIDs();
	     while(iter.hasNext()){
	    	 long uid=iter.nextLong();
	    	 List<RecommendedItem> list=r.recommend(uid, 3);
	    	 
	    	 System.out.println();
	    	 for (RecommendedItem item : list) {
	    		 System.out.println(""+item.getItemID());
				
			}
	    	 System.out.println("******");
	     }
	}

}
