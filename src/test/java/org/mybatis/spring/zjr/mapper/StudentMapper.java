package org.mybatis.spring.zjr.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public interface StudentMapper {

  @Select("select * from test")
  List<Map<String, Object>> list();
}
