package service

import (
	"sync"
	"time"
)

type Task struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	DueDate     string    `json:"due_date"`
	Done        bool      `json:"done"`
	CreatedAt   time.Time `json:"created_at"`
}

type TaskService struct {
	mu    sync.RWMutex
	tasks map[string]Task
}

func NewTaskService() *TaskService {
	return &TaskService{
		tasks: make(map[string]Task),
	}
}

func (s *TaskService) Create(task Task) Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := "t_" + time.Now().Format("150405.000")
	task.ID = id
	task.CreatedAt = time.Now()
	s.tasks[id] = task
	return task
}

func (s *TaskService) GetAll() []Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		list = append(list, t)
	}
	return list
}

func (s *TaskService) GetByID(id string) (Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	return t, ok
}

func (s *TaskService) Update(id string, updated Task) (Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return Task{}, false
	}
	if updated.Title != "" {
		t.Title = updated.Title
	}
	if updated.Description != "" {
		t.Description = updated.Description
	}
	if updated.DueDate != "" {
		t.DueDate = updated.DueDate
	}
	t.Done = updated.Done
	s.tasks[id] = t
	return t, true
}

func (s *TaskService) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.tasks[id]
	if ok {
		delete(s.tasks, id)
	}
	return ok
}
