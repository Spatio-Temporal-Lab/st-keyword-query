import com.START.STKQ.constant.Constant;

public class test1 {

    public static void main(String[] args) {
        int needByteCountForS = Constant.SPATIAL_BYTE_COUNT - Constant.FILTER_LEVEL / 4;
        int needByteCountForT = Constant.TIME_BYTE_COUNT - Constant.FILTER_LEVEL / 8;
        System.out.println(needByteCountForS + " " + needByteCountForT);
    }
}
