package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	pgxuuid "github.com/jackc/pgx-gofrs-uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tryy3/cozytemp-calibration-service/config"
	"github.com/tryy3/cozytemp-service-helper/kafka"
	"github.com/tryy3/cozytemp-service-helper/models"
)

func main() {
	cfg := config.Load()

	log.Printf("Starting CozyTemp Calibration Service")
	log.Printf("Kafka Brokers: %v", cfg.KafkaBrokers)
	log.Printf("Consumer Topic: %s", cfg.KafkaConsumerTopic)
	log.Printf("Producer Topic: %s", cfg.KafkaProducerTopic)
	log.Printf("Consumer Group: %s", cfg.KafkaConsumerGroup)
	consumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerTopic, cfg.KafkaConsumerGroup)
	defer consumer.Close()

	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaProducerTopic)
	defer producer.Close()

	for {
		msg, err := consumer.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		var calibrationData models.BeforeCalibrationDataEvent
		err = json.Unmarshal(msg.Value, &calibrationData)
		if err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			continue
		}
		log.Printf("Calibration Data: %v", calibrationData)

		calibratedTemperature, err := calibrateData(cfg, &calibrationData)
		if err != nil {
			log.Printf("Error calibrating data: %v", err)
			continue
		}
		log.Printf("Data calibrated successfully")

		afterCalibrationData := models.AfterCalibrationDataEvent{
			NodeIdentifier:        calibrationData.NodeIdentifier,
			NodeID:                calibrationData.NodeID,
			SensorIdentifier:      calibrationData.SensorIdentifier,
			SensorID:              calibrationData.SensorID,
			RawDataID:             calibrationData.RawDataID,
			RawTemperature:        calibrationData.RawTemperature,
			CalibratedTemperature: calibratedTemperature,
		}
		log.Printf("After Calibration Data: %v", afterCalibrationData)

		err = insertCalibratedData(cfg, &afterCalibrationData)
		if err != nil {
			log.Printf("Error inserting calibrated data: %v", err)
			continue
		}
		log.Printf("Calibrated data inserted successfully")

		jsonData, err := json.Marshal(afterCalibrationData)
		if err != nil {
			log.Printf("Error marshalling after calibration data: %v", err)
			continue
		}

		if calibrationData.NodeIdentifier != "" {
			err = producer.WriteMessage(context.Background(), []byte(calibrationData.NodeIdentifier), jsonData)
			if err != nil {
				log.Printf("Error producing message: %v", err)
				continue
			}
			log.Printf("Message produced successfully")
		} else {
			log.Printf("Node identifier is empty, skipping message production")
		}

		err = consumer.CommitMessages(context.Background(), msg)
		if err != nil {
			log.Printf("Error committing message: %v", err)
			continue
		}
		log.Printf("Message committed successfully")
	}
}

// TODO: For now hard code this values, but make it more dynamic in the future
var (
	sensorValues  = []float64{21.74, 10.64, 20.9733, 10.7386, 21.1756, 10.9283}
	controlValues = []float64{22.5, 12.2, 22.1, 12.2, 21.6, 12.4}
)

// Linear regression formula: y = ax + b
// A_values is the sensor values
// B_values is the control values
// Returns the slope (a) and intercept (b)
func linearRegression(A_values []float64, B_values []float64) (float64, float64) {
	n := len(A_values)
	var sumA, sumB, sumAB, sumASquared float64

	for i := 0; i < n; i++ {
		sumA += A_values[i]
		sumB += B_values[i]
		sumAB += A_values[i] * B_values[i]
		sumASquared += A_values[i] * A_values[i]
	}

	resultA := (float64(n)*sumAB - sumA*sumB) / (float64(n)*sumASquared - sumA*sumA)
	resultB := (sumB - resultA*sumA) / float64(n)

	return resultA, resultB
}

func calibrateData(cfg *config.Config, data *models.BeforeCalibrationDataEvent) (float64, error) {
	resultA, resultB := linearRegression(sensorValues, controlValues)
	log.Printf("Result A: %v", resultA)
	log.Printf("Result B: %v", resultB)

	calibratedTemperature := (resultA * data.RawTemperature) + resultB
	log.Printf("Calibrated Temperature: %v", calibratedTemperature)

	return calibratedTemperature, nil
}

func connectToPostgres(cfg *config.Config) (*pgxpool.Pool, error) {
	dbconfig, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("error parsing Postgres URL: %w", err)
	}
	dbconfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		pgxuuid.Register(conn.TypeMap())
		return nil
	}

	db, err := pgxpool.NewWithConfig(context.Background(), dbconfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Postgres: %w", err)
	}
	if err := db.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("error pinging Postgres: %w", err)
	}
	return db, nil
}

func insertCalibratedData(cfg *config.Config, data *models.AfterCalibrationDataEvent) error {
	db, err := connectToPostgres(cfg)
	if err != nil {
		log.Printf("Error connecting to Postgres: %v", err)
		return fmt.Errorf("error connecting to Postgres: %w", err)
	}
	defer db.Close()

	tx, err := db.Begin(context.Background())
	if err != nil {
		log.Printf("Error beginning transaction: %v", err)
		return fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	_, err = tx.Exec(context.Background(), "INSERT INTO calibrated_temperature (\"rawDataId\", \"temperature\") VALUES ($1, $2)", data.RawDataID, data.CalibratedTemperature)
	if err != nil {
		log.Printf("Error inserting calibrated data: %v", err)
		return fmt.Errorf("error inserting calibrated data: %w", err)
	}
	err = tx.Commit(context.Background())
	if err != nil {
		log.Printf("Error committing transaction: %v", err)
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}
