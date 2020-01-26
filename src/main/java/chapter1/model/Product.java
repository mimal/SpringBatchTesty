package chapter1.model;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.math.BigDecimal;

@Data
@Entity // to simplify..
public class Product {
    @Id
    @GeneratedValue
    private String id;
    private String name;
    private String description;
    private BigDecimal price;
}
