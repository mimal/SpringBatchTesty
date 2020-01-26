package chapter1.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity // to simplify..
public class Product {
    @Id
    @GeneratedValue
    private String id;
    private String name;
    private String description;
    private BigDecimal price;
}
