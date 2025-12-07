package storage

import model.Task
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVPrinter
import java.io.File
import java.io.FileReader
import java.io.FileWriter
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

/**
 * CSV-based task storage.
 * Provides simple persistence for task data using local CSV file.
 *
 * **Privacy note**: Data stored locally only. No cloud services.
 * **Thread safety**: Not thread-safe. Use single-threaded server or add locking.
 *
 * **CSV schema**:
 * ```
 * id,title,completed,created_at
 * 7a9f2c3d-...,\"Buy groceries\",false,2025-10-15T14:32:10
 * ```
 *
 * @property csvFile CSV file path (default: data/tasks.csv)
 */
class TaskStore(
    private val csvFile: File = File("data/tasks.csv"),
) {
    companion object {
        private val CSV_FORMAT =
            CSVFormat.DEFAULT
                .builder()
                .setHeader("id", "title", "completed", "created_at")
                .setSkipHeaderRecord(true)
                .build()

        private const val EMPTY_FILE_SIZE = 0L
    }

    init {
        // Create data directory and file if missing
        csvFile.parentFile?.mkdirs()
        if (!csvFile.exists()) {
            csvFile.createNewFile()
        }

        // Write CSV header if file is empty (for new files or test cases)
        if (csvFile.length() == EMPTY_FILE_SIZE) {
            FileWriter(csvFile).use { writer ->
                CSVPrinter(writer, CSV_FORMAT).use { printer ->
                    printer.printRecord("id", "title", "completed", "created_at")
                }
            }
        }
    }

    /**
     * Get all tasks from storage.
     *
     * @return List of tasks (empty if no tasks stored)
     */
    fun getAll(): List<Task> {
        if (!csvFile.exists() || csvFile.length() == EMPTY_FILE_SIZE) return emptyList()

        return FileReader(csvFile).use { reader ->
            CSVParser(reader, CSV_FORMAT).use { parser ->
                parser.mapNotNull { record ->
                    try {
                        Task(
                            id = record[0],
                            title = record[1],
                            completed = record[2].toBoolean(),
                            createdAt = LocalDateTime.parse(record[3], DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                        )
                    } catch (e: IndexOutOfBoundsException) {
                        // CSV row has missing fields - skip this row
                        System.err.println("Warning: Skipping CSV row with missing fields: ${record.toList()}")
                        null
                    } catch (e: DateTimeParseException) {
                        // Date format is invalid - skip this row
                        System.err.println("Warning: Skipping CSV row with invalid date: ${record.toList()}")
                        null
                    } catch (e: IllegalArgumentException) {
                        // Boolean parsing failed or other validation issue - skip this row
                        System.err.println("Warning: Skipping CSV row with invalid data: ${record.toList()}")
                        null
                    }
                }
            }
        } .sortedByDescending {it.createdAt}
    }

    /**
     * Get task by ID.
     *
     * @param id Task ID
     * @return Task if found, null otherwise
     */
    fun getById(id: String): Task? = getAll().find { it.id == id }

    /**
     * Add new task to storage.
     *
     * **Note**: Does not check for duplicate IDs. Caller should ensure uniqueness.
     *
     * @param task Task to add
     */
    fun add(task: Task) {
        // Ensure CSV file exists with header row
        if (!csvFile.exists() || csvFile.length() == EMPTY_FILE_SIZE) {
            csvFile.parentFile?.mkdirs()
            FileWriter(csvFile, false).use { writer ->
                writer.write("id,title,completed,created_at\n")
            }
        }

        FileWriter(csvFile, true).use { writer ->
            CSVPrinter(writer, CSV_FORMAT).use { printer ->
                printer.printRecord(
                    task.id,
                    task.title,
                    task.completed,
                    task.createdAt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                )
            }
        }
    }

    /**
     * Update existing task.
     * Replaces entire task with new data.
     *
     * **Implementation**: Reads all, replaces matching task, writes all.
     * Not efficient for large datasets, but simple and correct.
     *
     * @param task Task with updated data (ID must match existing task)
     * @return true if task found and updated, false if not found
     */
    fun update(task: Task): Boolean {
        val tasks = getAll().toMutableList()
        val index = tasks.indexOfFirst { it.id == task.id }

        if (index == -1) return false

        tasks[index] = task
        writeAll(tasks)
        return true
    }

    /**
     * Delete task by ID.
     *
     * @param id Task ID to delete
     * @return true if task found and deleted, false if not found
     */
    fun delete(id: String): Boolean {
        val tasks = getAll().toMutableList()
        val removed = tasks.removeIf { it.id == id }

        if (removed) {
            writeAll(tasks)
        }

        return removed
    }

    /**
     * Toggle task completion status.
     *
     * @param id Task ID
     * @return Updated task if found, null if not found
     */
    fun toggleComplete(id: String): Task? {
        val task = getById(id) ?: return null
        val updated = task.copy(completed = !task.completed)
        update(updated)
        return updated
    }

    /**
     * Search tasks by title (case-insensitive substring match).
     *
     * **Example**: query="invoice" matches "Pay invoice" and "Invoice review"
     *
     * @param query Search query
     * @return List of matching tasks
     */
    fun search(query: String): List<Task> {
        if (query.isBlank()) return getAll()

        val normalizedQuery = query.trim().lowercase()
        return getAll().filter { task ->
            task.title.lowercase().contains(normalizedQuery)
        }
    }

    /**
     * Write all tasks to CSV file (overwrites existing file).
     * Used by update() and delete() after modifying task list.
     *
     * @param tasks List of tasks to write
     */
    private fun writeAll(tasks: List<Task>) {
        FileWriter(csvFile, false).use { writer ->
            CSVPrinter(writer, CSV_FORMAT).use { printer ->
                printer.printRecord("id", "title", "completed", "created_at")
                tasks.forEach { task ->
                    printer.printRecord(
                        task.id,
                        task.title,
                        task.completed,
                        task.createdAt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    )
                }
            }
        }
    }

    /**
     * Clear all tasks (for testing).
     * Deletes CSV file and recreates with header only.
     */
    fun clear() {
        csvFile.delete()
        csvFile.createNewFile()
        FileWriter(csvFile).use { writer ->
            CSVPrinter(writer, CSV_FORMAT).use { printer ->
                printer.printRecord("id", "title", "completed", "created_at")
            }
        }
    }
}
