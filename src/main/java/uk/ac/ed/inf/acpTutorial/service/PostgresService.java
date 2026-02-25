package uk.ac.ed.inf.acpTutorial.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ed.inf.acpTutorial.dto.Drone;
import uk.ac.ed.inf.acpTutorial.entity.DroneEntity;
import uk.ac.ed.inf.acpTutorial.mapper.DroneMapper;
import uk.ac.ed.inf.acpTutorial.repository.DroneRepository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.Map;
@Slf4j
@Service
public class PostgresService {

    private final JdbcTemplate jdbcTemplate;
    private final DroneRepository droneRepository;

    public PostgresService(JdbcTemplate jdbcTemplate, DroneRepository droneRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.droneRepository = droneRepository;
    }

    /**
     * Retrieves all drones from the database and maps them to Drone DTOs.
     * @return List of Drone DTOs representing all drones in the database.
     */
    public List<Drone> getAllDrones() {
        return jdbcTemplate.query(
                "SELECT * FROM ilp.drones",
                    new BeanPropertyRowMapper<>(DroneEntity.class))
                .stream()
                    .map(DroneMapper::entityToDto)
                .toList();
    }

    @Transactional
    public String createDroneUsingJdbc(Drone drone) {
        var createDrone = DroneMapper.dtoToEntity(drone);
        createDrone.setId(UUID.randomUUID().toString());

        String sql = "INSERT INTO ilp.drones (id, name, cooling, heating, capacity, max_moves, cost_per_move, cost_initial, cost_final, description) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, createDrone.getId());
            ps.setString(2, createDrone.getName());
            ps.setBoolean(3, createDrone.isCooling());
            ps.setBoolean(4, createDrone.isHeating());
            ps.setBigDecimal(5, createDrone.getCapacity());
            ps.setInt(6, createDrone.getMaxMoves());
            ps.setBigDecimal(7, createDrone.getCostPerMove());
            ps.setBigDecimal(8, createDrone.getCostInitial());
            ps.setBigDecimal(9, createDrone.getCostFinal());
            ps.setString(10, createDrone.getDescription());
            return ps;
        });

        return createDrone.getId();
    }

    @Transactional
    public String createDroneUsingJpa(Drone drone) {
        var createDrone = DroneMapper.dtoToEntity(drone);
        createDrone.setId(UUID.randomUUID().toString());
        return droneRepository.save(createDrone).getId();
    }

    @Transactional
    public void deleteDrone(String droneId) {
        droneRepository.deleteById(droneId);
    }

    public List<Map<String, Object>> getAllRowsFromTable(String table) {
        return jdbcTemplate.queryForList("SELECT * FROM " + table);
    }
    public void insertDroneRow(String table, String name, String id,
                               boolean cooling, boolean heating,
                               double capacity, int maxMoves,
                               double costPerMove, double costInitial,
                               double costFinal, double costPer100Moves) {
        jdbcTemplate.update(
                "INSERT INTO " + table +
                        " (name, id, cooling, heating, capacity, maxMoves, costPerMove, costInitial, costFinal, costPer100Moves) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                name, id, cooling, heating, capacity, maxMoves,
                costPerMove, costInitial, costFinal, costPer100Moves
        );
    }
}
