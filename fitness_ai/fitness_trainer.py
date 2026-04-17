"""
Fitness AI Trainer - Neural Network for Exercise Analysis and Recommendations
"""

import numpy as np
from typing import Dict, List, Tuple
import json

class FitnessTrainerAI:
    def __init__(self):
        self.exercise_models = {}
        self.user_profiles = {}
        self.recommendation_engine = RecommendationEngine()
        
    def analyze_exercise(self, exercise_name: str, keypoints: List[Dict]) -> Dict:
        """
        Analyze exercise technique using pose keypoints
        keypoints: list of body joint coordinates from pose estimation
        """
        if exercise_name not in self.exercise_models:
            self.exercise_models[exercise_name] = ExerciseModel(exercise_name)
        
        model = self.exercise_models[exercise_name]
        return model.analyze(keypoints)
    
    def get_workout_plan(self, user_id: str, fitness_level: str, goals: List[str]) -> Dict:
        """Generate personalized workout plan"""
        if user_id not in self.user_profiles:
            self.user_profiles[user_id] = UserProfile(user_id)
        
        profile = self.user_profiles[user_id]
        profile.update_fitness_level(fitness_level)
        profile.update_goals(goals)
        
        return self.recommendation_engine.generate_plan(profile)
    
    def provide_feedback(self, exercise_analysis: Dict) -> str:
        """Provide natural language feedback based on analysis"""
        feedback_generator = FeedbackGenerator()
        return feedback_generator.generate(exercise_analysis)


class ExerciseModel:
    def __init__(self, exercise_name: str):
        self.exercise_name = exercise_name
        self.angle_thresholds = self._load_thresholds()
        
    def _load_thresholds(self) -> Dict:
        """Load angle thresholds for proper form"""
        thresholds = {
            'squat': {
                'knee_angle': (80, 100),  # degrees
                'hip_angle': (70, 90),
                'back_angle': (85, 95)
            },
            'pushup': {
                'elbow_angle': (80, 100),
                'body_straightness': (170, 180)
            },
            'deadlift': {
                'back_angle': (80, 90),
                'knee_angle': (150, 170),
                'hip_angle': (60, 80)
            }
        }
        return thresholds.get(self.exercise_name, {})
    
    def analyze(self, keypoints: List[Dict]) -> Dict:
        """Analyze exercise form and provide scores"""
        if not keypoints:
            return {'error': 'No keypoints provided'}
        
        angles = self._calculate_angles(keypoints)
        form_score = self._calculate_form_score(angles)
        corrections = self._suggest_corrections(angles)
        
        return {
            'exercise': self.exercise_name,
            'angles': angles,
            'form_score': form_score,
            'corrections': corrections,
            'is_valid': form_score > 0.7
        }
    
    def _calculate_angles(self, keypoints: List[Dict]) -> Dict:
        """Calculate joint angles from keypoints"""
        # Simplified angle calculation for demo
        # In production, use proper vector math between joints
        angles = {}
        
        if len(keypoints) >= 3:
            # Example: calculate knee angle for squat
            if self.exercise_name == 'squat':
                angles['knee_angle'] = 95  # Simulated value
                angles['hip_angle'] = 85
                angles['back_angle'] = 90
        
        return angles
    
    def _calculate_form_score(self, angles: Dict) -> float:
        """Calculate overall form score (0-1)"""
        if not angles or not self.angle_thresholds:
            return 0.5
        
        scores = []
        for angle_name, value in angles.items():
            if angle_name in self.angle_thresholds:
                min_val, max_val = self.angle_thresholds[angle_name]
                if min_val <= value <= max_val:
                    scores.append(1.0)
                else:
                    deviation = min(abs(value - min_val), abs(value - max_val))
                    score = max(0.0, 1.0 - deviation / 30)  # Penalize deviations
                    scores.append(score)
        
        return float(np.mean(scores)) if scores else 0.5
    
    def _suggest_corrections(self, angles: Dict) -> List[str]:
        """Suggest corrections based on angle analysis"""
        corrections = []
        
        for angle_name, value in angles.items():
            if angle_name in self.angle_thresholds:
                min_val, max_val = self.angle_thresholds[angle_name]
                if value < min_val:
                    corrections.append(f"Increase {angle_name.replace('_', ' ')} (current: {value}°, min: {min_val}°)")
                elif value > max_val:
                    corrections.append(f"Decrease {angle_name.replace('_', ' ')} (current: {value}°, max: {max_val}°)")
        
        return corrections


class UserProfile:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.fitness_level = 'beginner'
        self.goals = []
        self.workout_history = []
        self.preferences = {}
    
    def update_fitness_level(self, level: str):
        self.fitness_level = level
    
    def update_goals(self, goals: List[str]):
        self.goals = goals
    
    def add_workout(self, workout_data: Dict):
        self.workout_history.append(workout_data)


class RecommendationEngine:
    def __init__(self):
        self.exercise_database = self._load_exercises()
    
    def _load_exercises(self) -> Dict:
        """Load exercise database with difficulty levels"""
        return {
            'beginner': ['squat', 'pushup', 'plank', 'lunges'],
            'intermediate': ['deadlift', 'bench_press', 'pull_ups', 'burpees'],
            'advanced': ['muscle_up', 'pistol_squat', 'handstand_pushup']
        }
    
    def generate_plan(self, profile: UserProfile) -> Dict:
        """Generate personalized workout plan"""
        level = profile.fitness_level
        exercises = self.exercise_database.get(level, self.exercise_database['beginner'])
        
        # Simple plan generation logic
        plan = {
            'user_id': profile.user_id,
            'fitness_level': level,
            'goals': profile.goals,
            'workout_days': 3,
            'exercises': [],
            'duration_weeks': 4
        }
        
        for i, exercise in enumerate(exercises[:4]):  # Top 4 exercises for the level
            sets = 3 if level == 'beginner' else 4
            reps = 10 if level == 'beginner' else 12
            
            plan['exercises'].append({
                'name': exercise,
                'sets': sets,
                'reps': reps,
                'rest_seconds': 60,
                'day': (i % 3) + 1
            })
        
        return plan


class FeedbackGenerator:
    def __init__(self):
        self.feedback_templates = {
            'good': [
                "Отличная техника! Продолжай в том же духе.",
                "Форма выполнения превосходная!",
                "Замечательно! Ты хорошо контролируешь движение."
            ],
            'needs_improvement': [
                "Обрати внимание на угол в коленях.",
                "Попробуй держать спину ровнее.",
                "Следи за глубиной приседа."
            ],
            'warning': [
                "Будь осторожен, есть риск травмы.",
                "Снизь вес и сосредоточься на технике.",
                "Рекомендую проконсультироваться с тренером."
            ]
        }
    
    def generate(self, analysis: Dict) -> str:
        """Generate natural language feedback"""
        if 'error' in analysis:
            return "Не удалось проанализировать упражнение. Попробуй еще раз."
        
        score = analysis.get('form_score', 0.5)
        corrections = analysis.get('corrections', [])
        
        if score > 0.8:
            feedback = np.random.choice(self.feedback_templates['good'])
        elif score > 0.6:
            feedback = np.random.choice(self.feedback_templates['needs_improvement'])
            if corrections:
                feedback += f" Рекомендации: {', '.join(corrections[:2])}"
        else:
            feedback = np.random.choice(self.feedback_templates['warning'])
            if corrections:
                feedback += f" Важно исправить: {', '.join(corrections)}"
        
        return feedback


# Demo usage
if __name__ == "__main__":
    trainer = FitnessTrainerAI()
    
    # Simulate exercise analysis
    print("=== Fitness AI Trainer Demo ===\n")
    
    # Analyze squat exercise
    squat_keypoints = [{'joint': 'hip', 'x': 100, 'y': 150}, 
                       {'joint': 'knee', 'x': 110, 'y': 200},
                       {'joint': 'ankle', 'x': 120, 'y': 250}]
    
    analysis = trainer.analyze_exercise('squat', squat_keypoints)
    print(f"Exercise Analysis: {json.dumps(analysis, indent=2)}")
    
    feedback = trainer.provide_feedback(analysis)
    print(f"\nFeedback: {feedback}")
    
    # Generate workout plan
    plan = trainer.get_workout_plan('user_001', 'beginner', ['weight_loss', 'strength'])
    print(f"\nWorkout Plan: {json.dumps(plan, indent=2)}")
