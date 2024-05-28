package com.jzo2o.market.service.impl;

import cn.hutool.db.DbRuntimeException;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.injector.methods.SelectById;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.jzo2o.api.market.dto.request.CouponUseBackReqDTO;
import com.jzo2o.api.market.dto.request.CouponUseReqDTO;
import com.jzo2o.api.market.dto.response.AvailableCouponsResDTO;
import com.jzo2o.api.market.dto.response.CouponUseResDTO;
import com.jzo2o.common.expcetions.BadRequestException;
import com.jzo2o.common.expcetions.CommonException;
import com.jzo2o.common.expcetions.DBException;
import com.jzo2o.common.model.PageResult;
import com.jzo2o.common.utils.*;
import com.jzo2o.market.enums.ActivityStatusEnum;
import com.jzo2o.market.enums.CouponStatusEnum;
import com.jzo2o.market.mapper.CouponMapper;
import com.jzo2o.market.model.domain.Activity;
import com.jzo2o.market.model.domain.Coupon;
import com.jzo2o.market.model.domain.CouponWriteOff;
import com.jzo2o.market.model.dto.request.CouponOperationPageQueryReqDTO;
import com.jzo2o.market.model.dto.request.SeizeCouponReqDTO;
import com.jzo2o.market.model.dto.response.ActivityInfoResDTO;
import com.jzo2o.market.model.dto.response.CouponInfoResDTO;
import com.jzo2o.market.service.IActivityService;
import com.jzo2o.market.service.ICouponService;
import com.jzo2o.market.service.ICouponUseBackService;
import com.jzo2o.market.service.ICouponWriteOffService;
import com.jzo2o.market.utils.CouponUtils;
import com.jzo2o.mvc.utils.UserContext;
import com.jzo2o.mysql.utils.PageUtils;
import com.jzo2o.redis.utils.RedisSyncQueueUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.jzo2o.common.constants.ErrorInfo.Code.SEIZE_COUPON_FAILD;
import static com.jzo2o.market.constants.RedisConstants.RedisKey.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author itcast
 * @since 2023-09-16
 */
@Service
@Slf4j
public class CouponServiceImpl extends ServiceImpl<CouponMapper, Coupon> implements ICouponService {

    @Resource(name = "seizeCouponScript")
    private DefaultRedisScript<String> seizeCouponScript;

    @Resource
    private RedisTemplate redisTemplate;

    @Resource
    private IActivityService activityService;

    @Resource
    private ICouponUseBackService couponUseBackService;

    @Resource
    private ICouponWriteOffService couponWriteOffService;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CouponUseResDTO use(CouponUseReqDTO couponUseReqDTO) {
        //判空
        if (ObjectUtils.isNull(couponUseReqDTO.getOrdersId()) ||
                ObjectUtils.isNull(couponUseReqDTO.getTotalAmount()))
        {
            throw new BadRequestException("优惠券核销的订单信息为空");
        }
        Long userId = UserContext.currentUserId();
        Coupon coupon = getById(couponUseReqDTO.getId());
        // 优惠券判空
        if (coupon == null ) {
            throw new BadRequestException("优惠券不存在");
        }
        if ( ObjectUtils.notEqual(coupon.getUserId(),userId)) {
            throw new BadRequestException("只允许核销自己的优惠券");
        }
        //更新优惠券表的状态
        boolean update = lambdaUpdate()
                .eq(Coupon::getId, couponUseReqDTO.getId())
                .eq(Coupon::getStatus, CouponStatusEnum.NO_USE.getStatus())
                .gt(Coupon::getValidityTime, LocalDateTime.now())
                .le(Coupon::getAmountCondition, couponUseReqDTO.getTotalAmount())//满减金额小于总金额 才可以使用该优惠券
                .set(Coupon::getOrdersId, couponUseReqDTO.getOrdersId())
                .set(Coupon::getStatus, CouponStatusEnum.USED.getStatus())
                .set(Coupon::getUseTime, DateUtils.now())
                .update();
        if(!update){
            throw new DBException("优惠券核销失败");
        }
        //添加核销记录
        CouponWriteOff couponWriteOff = CouponWriteOff.builder()
                .id(IdUtils.getSnowflakeNextId())
                .couponId(couponUseReqDTO.getId())
                .userId(userId)
                .ordersId(couponUseReqDTO.getOrdersId())
                .activityId(coupon.getActivityId())
                .writeOffTime(DateUtils.now())
                .writeOffManName(coupon.getUserName())
                .writeOffManPhone(coupon.getUserPhone())
                .build();
        if(!couponWriteOffService.save(couponWriteOff)){
            throw new DBException("优惠券核销失败");
        }
        //计算优惠金额并返回
        BigDecimal discountAmount = CouponUtils.calDiscountAmount(coupon, couponUseReqDTO.getTotalAmount());
        CouponUseResDTO couponUseResDTO = new CouponUseResDTO();
        couponUseResDTO.setDiscountAmount(discountAmount);
        return couponUseResDTO;
    }

    @Override
    public PageResult<CouponInfoResDTO> queryForPageOfOperation(CouponOperationPageQueryReqDTO couponOperationPageQueryReqDTO) {
        // 1.数据校验
        if (ObjectUtils.isNull(couponOperationPageQueryReqDTO.getActivityId())) {
            throw new BadRequestException("请指定活动");
        }
        // 2.数据查询
        // 分页 排序
        Page<Coupon> couponQueryPage = PageUtils.parsePageQuery(couponOperationPageQueryReqDTO, Coupon.class);
        // 查询条件
        LambdaQueryWrapper<Coupon> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.eq(Coupon::getActivityId, couponOperationPageQueryReqDTO.getActivityId());
        // 查询数据
        Page<Coupon> couponPage = baseMapper.selectPage(couponQueryPage, lambdaQueryWrapper);

        // 3.数据转化，并返回
        return PageUtils.toPage(couponPage, CouponInfoResDTO.class);
    }

    @Override
    public List<CouponInfoResDTO> queryForList(Long lastId, Long userId, Integer status) {

        // 1.校验
        if (status > 3 || status < 1) {
            throw new BadRequestException("请求状态不存在");
        }
        // 2.查询准备
        LambdaQueryWrapper<Coupon> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        // 查询条件
        lambdaQueryWrapper.eq(Coupon::getStatus, status)
                .eq(Coupon::getUserId, userId)
                .lt(ObjectUtils.isNotNull(lastId), Coupon::getId, lastId);
        // 查询字段
        lambdaQueryWrapper.select(Coupon::getId);
        // 排序
        lambdaQueryWrapper.orderByDesc(Coupon::getId);
        // 查询条数限制
        lambdaQueryWrapper.last(" limit 10 ");
        // 3.查询数据(数据中只含id)
        List<Coupon> couponsOnlyId = baseMapper.selectList(lambdaQueryWrapper);
        //判空
        if (CollUtils.isEmpty(couponsOnlyId)) {
            return new ArrayList<>();
        }

        // 4.获取数据且数据转换
        // 优惠id列表
        List<Long> ids = couponsOnlyId.stream()
                .map(Coupon::getId)
                .collect(Collectors.toList());
        // 获取优惠券数据
        List<Coupon> coupons = baseMapper.selectBatchIds(ids);
        // 数据转换
        return BeanUtils.copyToList(coupons, CouponInfoResDTO.class);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void revoke(Long activityId) {
        lambdaUpdate()
                .set(Coupon::getStatus, CouponStatusEnum.VOIDED.getStatus())
                .eq(Coupon::getActivityId, activityId)
                .eq(Coupon::getStatus, CouponStatusEnum.NO_USE.getStatus())
                .update();
    }

    @Override
    public Integer countReceiveNumByActivityId(Long activityId) {
        return lambdaQuery().eq(Coupon::getActivityId, activityId)
                .count();
    }

    @Override
    public void processExpireCoupon() {
        lambdaUpdate()
                .set(Coupon::getStatus, CouponStatusEnum.INVALID.getStatus())
                .eq(Coupon::getStatus, CouponStatusEnum.NO_USE.getStatus())
                .le(Coupon::getValidityTime, DateUtils.now())
                .update();
    }

    @Override
    public void seizeCoupon(SeizeCouponReqDTO seizeCouponReqDTO) {
        // 1.校验活动开始时间或结束
        // 首先从缓存查询活动
        ActivityInfoResDTO activity  = activityService.getActivityInfoByIdFromCache(seizeCouponReqDTO.getId());
        LocalDateTime now = LocalDateTime.now();
        if(ObjectUtils.isNull(activity) ||  activity.getDistributeStartTime().isAfter(now)){
            throw new CommonException(SEIZE_COUPON_FAILD, "活动未开始");
        }
        if(activity.getDistributeEndTime().isBefore(now)){
            throw new CommonException(SEIZE_COUPON_FAILD, "活动已结束");
        }
        // 2.抢券准备
//         key: 抢券同步队列，资源库存,抢券列表
//         argv：抢券id,用户id
        int index = (int) (seizeCouponReqDTO.getId() % 10);//根据活动id余数决定队列
        // 同步队列redisKey
        String couponSeizeSyncRedisKey = RedisSyncQueueUtils.getQueueRedisKey(COUPON_SEIZE_SYNC_QUEUE_NAME, index);
        // 资源库存redisKey
        String resourceStockRedisKey = String.format(COUPON_RESOURCE_STOCK, index);
        // 抢券列表
        String couponSeizeListRedisKey = String.format(COUPON_SEIZE_LIST, activity.getId(), index);
        log.debug("seize coupon keys -> couponSeizeListRedisKey->{},resourceStockRedisKey->{},couponSeizeListRedisKey->{},seizeCouponReqDTO.getId()->{},UserContext.currentUserId():{}",
                couponSeizeListRedisKey, resourceStockRedisKey, couponSeizeListRedisKey, seizeCouponReqDTO.getId(), UserContext.currentUserId());
        // 3.执行lua脚本进行抢券结果
        Object execute = redisTemplate.execute(seizeCouponScript, Arrays.asList(couponSeizeSyncRedisKey, resourceStockRedisKey, couponSeizeListRedisKey), seizeCouponReqDTO.getId(), UserContext.currentUserId());
        log.debug("seize coupon result : {}", execute);

        // 4.处理lua脚本结果，失败的抛出异常，成功的正常返回
        if (execute == null) {
            throw new CommonException(SEIZE_COUPON_FAILD, "抢券失败");
        }
        long result = NumberUtils.parseLong(execute.toString());
        if (result > 0) {
            return;
        }
        if (result == -1) {
            throw new CommonException(SEIZE_COUPON_FAILD, "限领一张");
        }
        if (result == -2 || result == -4) {
            throw new CommonException(SEIZE_COUPON_FAILD, "已抢光!");
        }
        throw new CommonException(SEIZE_COUPON_FAILD, "抢券失败");
    }

    @Override
    public List<AvailableCouponsResDTO> getAvailable(BigDecimal totalAmount) {
        Long userId = UserContext.currentUserId();
        List<Coupon> coupons = lambdaQuery()//查询优惠券列表
                .eq(Coupon::getUserId, userId)
                .gt(Coupon::getValidityTime, DateUtils.now())
                .eq(Coupon::getStatus, CouponStatusEnum.NO_USE.getStatus())
                .le(Coupon::getAmountCondition, totalAmount)
                .list();
        if(CollUtils.isEmpty(coupons)){
            return new ArrayList<>();
        }

        List<AvailableCouponsResDTO> availableCouponsResDTOS = coupons.stream()
                .peek(coupon -> coupon.setDiscountAmount(CouponUtils.calDiscountAmount(coupon, totalAmount)))
                //过滤掉 优惠金额大于订单金额的优惠券
                .filter(coupon ->
                        coupon.getDiscountAmount().compareTo(new BigDecimal(0)) > 0
                                && coupon.getDiscountAmount().compareTo(totalAmount) < 0)
                //类型转换
                .map(coupon -> BeanUtils.copyBean(coupon, AvailableCouponsResDTO.class))
                //按优惠金额降序排列
                .sorted(Comparator.comparing(AvailableCouponsResDTO::getDiscountAmount).reversed())
                .collect(Collectors.toList());

        return availableCouponsResDTOS;
    }
}
