package com.jzo2o.market.handler;

import com.jzo2o.api.customer.CommonUserApi;
import com.jzo2o.api.customer.dto.response.CommonUserResDTO;
import com.jzo2o.common.utils.BeanUtils;
import com.jzo2o.common.utils.DateUtils;
import com.jzo2o.common.utils.IdUtils;
import com.jzo2o.common.utils.NumberUtils;
import com.jzo2o.market.enums.CouponStatusEnum;
import com.jzo2o.market.model.domain.Activity;
import com.jzo2o.market.model.domain.Coupon;
import com.jzo2o.market.service.IActivityService;
import com.jzo2o.market.service.ICouponService;
import com.jzo2o.redis.handler.SyncProcessHandler;
import com.jzo2o.redis.model.SyncMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;

import static com.jzo2o.market.constants.RedisConstants.RedisKey.COUPON_SEIZE_SYNC_QUEUE_NAME;

/**
 * @author zwy
 * @version 1.0
 * @description: TODO
 * @date 2024/5/28 15:17
 */
/**
 * 抢单成功同步任务
 */
@Component(COUPON_SEIZE_SYNC_QUEUE_NAME)
@Slf4j
public class SeizeCouponSyncProcessHandler implements SyncProcessHandler<Object> {

    @Resource
    private IActivityService activityService;
    @Resource
    private CommonUserApi commonUserApi;
    @Resource
    private ICouponService couponService;

    @Override
    public void batchProcess(List<SyncMessage<Object>> multiData) {
        throw new RuntimeException("不支持批量处理");
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void singleProcess(SyncMessage<Object> singleData) {
        log.info("获取需要同步的数据：{}", singleData);
        long userId = NumberUtils.parseLong(singleData.getKey()); //获取键userid
        long activityId = NumberUtils.parseLong(singleData.getValue().toString());
        log.info("userId={},activity={}",userId,activityId);
        Activity activity = activityService.getById(activityId);
        if(activity == null){
            return;
        }
        CommonUserResDTO userInfo = commonUserApi.findById(userId);//调用api获取用户信息
        if(userInfo == null){
            return;
        }
        //向优惠券表插入数据
        Coupon coupon = BeanUtils.toBean(activity, Coupon.class);//活动相关的信息可以直接拷贝
        coupon.setActivityId(activityId);
        coupon.setUserId(userId);
        coupon.setUserName(userInfo.getNickname());
        coupon.setUserPhone(userInfo.getPhone());
        coupon.setValidityTime(DateUtils.now().plusDays(activity.getValidityDays()));
        coupon.setStatus(CouponStatusEnum.NO_USE.getStatus());
        coupon.setId(IdUtils.getSnowflakeNextId());//雪花算法设置id
        couponService.save(coupon);
        //扣减数据库表中的库存
        activityService.deductStock(activityId);
    }
}
