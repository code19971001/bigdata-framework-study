package com.itcode.user_visit_action.req1

/**
 * @author : code1997
 * @date : 2022/2/25 23:32
 */
case class UserVisitAction(
                            date: String,
                            user_id: Long,
                            session_id: String,
                            page_id: Long,
                            action_time: String,
                            search_keyword: String,
                            click_category_id: Long,
                            click_product_od: Long,
                            order_category_ids: String,
                            order_product_ids: String,
                            pay_category_ids: String,
                            pay_product_ids: String,
                            city_id: String
                          )
