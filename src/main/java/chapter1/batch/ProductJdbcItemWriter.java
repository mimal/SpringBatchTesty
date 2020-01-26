package chapter1.batch;

import chapter1.model.Product;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

public class ProductJdbcItemWriter implements ItemWriter<Product> {
    private static final String INSERT_PRODUCT = "insert into product "+
            "(id,name,description,price) values(?,?,?,?)";

    private static final String UPDATE_PRODUCT = "update product set "+
            "name=?, description=?, price=? where id=?";

    private JdbcTemplate jdbcTemplate;

    public ProductJdbcItemWriter(DataSource ds) {
        this.jdbcTemplate = new JdbcTemplate(ds);
    }

    @Override
    public void write(List<? extends Product> items) throws Exception {
        for(Product item: items){
            int updated = jdbcTemplate.update(
                    UPDATE_PRODUCT,
                    item.getName(),item.getDescription(),
                    item.getPrice(),item.getId()
            );
            if (updated == 0) {
                jdbcTemplate.update(
                        INSERT_PRODUCT,
                        item.getId(),item.getName(),
                         item.getDescription(),item.getPrice()
                );
            }
        }
    }
}
