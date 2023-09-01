package Entitys;


import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
@Builder
public class targetTable {
    private  String  tabletype;
    private  String  tablename;
}
