import java.io.File;
import java.io.FileWriter;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.math3.distribution.NormalDistribution;


class Order{
	// Order ID
	private int id;
	// Order Timestamp
	private String orderTime;
	// Order Size
	private int Size;
	// price
	private double price;
	//Order Direction 0：Buy 1：Sell
	private int flag; 
	
	public Order(int id, String orderTime, int size, double price, int flag) {
		super();
		this.id = id;
		this.orderTime = orderTime;
		Size = size;
		this.price = price;
		this.flag = flag;
	}
	
	public Order(){

	}
	
	//getter and setter methods
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getOrderTime() {
		return orderTime;
	}
	public void setOrderTime(String orderTime) {
		this.orderTime = orderTime;
	}
	public int getSize() {
		return Size;
	}
	public void setSize(int size) {
		Size = size;
	}
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}
	public String getFlag() {
		return flag==0?"Buy":"Sell";
	}
	public void setFlag(int flag) {
		this.flag = flag;
	}
}

//Pricing Thread，implemented with Callable
class PriceThread implements Callable<Double>{
	//closing price of previous day
	private static final double PRICE1=OrderService.createRandom(5,10);
	//closing price of current day
	private static double PRICE2=0;
	//OrderService object for order generation
	private OrderService os;
	//current time, used for price updating/matching
	private int currentTime;
	//order direction
	private int flag;
	
	//Construct PriceThread
	public PriceThread(OrderService os,int currentTime,int flag){
		this.os=os;
		this.currentTime=currentTime;
		this.flag=flag;
	}
	
	//Start the Thread with call()
	@Override
	public Double call() throws Exception {
		//Pre-market time: 00:00~09:30
		if(currentTime>0 && currentTime<=9.5*60*60){
			//if before market open，return closing price from previous day
			//but since we are only simulating for 1 day, it's just a random value.
			return PriceThread.PRICE1;
			//During market hour (09:30~16:00)，price the call option with Blackscholes
		}else if (currentTime<=16*60*60){
		    NormalDistribution normalDistributioin = new NormalDistribution(); 
		    //some hard-coded intial values for option pricing
	        double S0 = 100, N = 23400, max = 86400; // seconds during market hours
	        double mu = 0.15, sigma = 0.2, K = 90.0, M = 1.0, r = 0.01, d = 0.0,dt = 1/ 100000;
	        Random rnd = new Random();
	        double W = Math.sqrt(dt) * rnd.nextGaussian(); // Brownian Motion Component
	        double drift = (mu - 0.5*sigma*2)*dt;
	        double diffusion = sigma*W*dt;
	        S0 = S0 * Math.exp(drift+diffusion);
	        double d1 = (Math.log(S0/K)+((r-d)+sigma*sigma/2.)*M)/(sigma*Math.sqrt(M));
	        double d2 = d1-sigma*Math.sqrt(M);
	        //normalDistributioin.cumulativeProbability for normal cdf
	        double p = S0*Math.exp(-d*M)*normalDistributioin.cumulativeProbability(d1)-K*Math.exp(-r*M)*normalDistributioin.cumulativeProbability(d2); 
	        //update potential closing price before return
	        PriceThread.PRICE2 = p;
	        return p;
		}
		//after market closed at 16:00, closing price
		return PriceThread.PRICE2;
	}
	
}


// OrderService Class for order generation on main thread 
public class OrderService  {
	//random number generator
	public static double createRandom(double min,double max){
		  return  (max-min+1)*Math.random() + min;
	}


	public static double getFinalPrice(int flag,double p){
        Random rnd = new Random();
		float spread = rnd.nextFloat();
	        //if order direction is "Buy", get ASK price
	        if(flag==0)
	         p = Math.round(100 * (p + spread / 2)) / 100.0;
	        else{
	        	//if order direction is "Sell", get BID price
	        	p = Math.round(100 * (p - spread / 2)) / 100.0;
	        }
	        return p;
	}
	// Poisson Process for order arrival simulation
	public static double poissonRand(double vLamda){     
			 double vx=0,vb=1,vc=Math.exp(-vLamda),vu; 
			 do {
					  vu=Math.random();
					  vb *=vu;
					  if(vb>=vc)
					   vx++;
			  }while(vb>=vc);
			 return vx;
		 }
	
  //core, used for order generation in main thread
	public  Map<Integer, Order> createOrder(){
		//define ExecutorService thread pool with initial value 50
		ExecutorService pool = Executors.newFixedThreadPool(50);
		try{
			//"orderBook" hashmap to store order information 
			Map<Integer, Order> orderBook=new HashMap<Integer, Order>();
			//timeTrack is the time tacker starting from 0 to 864000 seconds throughout the day
			int timeTrack=0;
			//use counter to count for number of order, which would also be used as ID
			int counter=0;
			//bound the timeTrack within a day 
			while(timeTrack<24*60*60){
				//waitTime is the passage of time before next order arrival
				//which is a poisson process
				int waitTime=(int)poissonRand(1);
				//increment the timeTrack by the waitTime
				timeTrack+=waitTime;
				//increment order counter by 1
				counter++;
				//format order timestamp as 00:00:00
			     String orderTime="";
			     if(timeTrack==0){
			    	 orderTime="00:00:00";
			     }else{
			    	 orderTime=LocalTime.MIN.plusSeconds(timeTrack).toString();
			     }
			      // order size is randomly drawn from (1, 100)
				  int size=(int)createRandom(1,100);
				  // order direction is either buy or sell 
				  int flag=(int)createRandom(1,100)%2==0?0:1;
				  //use pool.submit method to submit pricing thread
				  Future<Double>  fp=pool.submit(new PriceThread(this,timeTrack,flag));
				  //get price from the pricing thread
				  double orderPrice=OrderService.getFinalPrice(flag,fp.get());
				  //create order from Order object
				  Order order=new Order(counter,orderTime,size,orderPrice,flag);
				  //put order information into "orderBook" Hashmap
				  orderBook.put(counter, order);
			}
			//return the HashMap
			return orderBook;
		}catch(Exception e){
			return null;
		}finally {
			//shut down thread pool
			pool.shutdown();
		}
		
	}
 
	// enter main
	public static void main(String[] args)   throws Exception{
		  // start to write file
		  FileWriter fw=new FileWriter(new File("order.txt"));
		  // write header 
		  fw.write("ID\tTime\t\tSize\tDirection\tPrice\tValue\n");
		// Instantiate OrderService Object 
		OrderService os=new OrderService();
		// Call createOrder method to create order object
		Map<Integer, Order> orderBook=os.createOrder();
		//get info of all orders 
		Set<Integer> keys=orderBook.keySet();
		// Loop through order information
		for(Integer key:keys){
			//get item from Order object 
			 Order item=orderBook.get(key);
			 String msg=String.format("%d\t%s\t%d\t\t%-12s%.2f\t%.2f\n",item.getId(),item.getOrderTime(),item.getSize(),
					   item.getFlag(),item.getPrice(),item.getSize()*item.getPrice());
			  //print log
			  System.out.println(msg);
			  //also write into orderbook
			  fw.write(msg); 
		}
		//close file 
		fw.close();
	}

}
