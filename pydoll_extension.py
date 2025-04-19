import asyncio

import numpy as np
from pydoll.element import InputCommands, RuntimeCommands

class HumanInputCommands(InputCommands):
    @classmethod
    def mouse_move(cls, x: int, y: int) -> dict:
        """Generate mouse movement command"""
        command = cls.CLICK_ELEMENT_TEMPLATE.copy()
        command['params'] = {
            'type': 'mouseMoved',
            'x': x,
            'y': y,
            'modifiers': 0
        }
        return command

class HumanWebElement:
    """Wrapper adding human-like interactions to existing WebElements"""
    def __init__(self, element):
        self._wrapped = element
        
    def __getattr__(self, name):
        """Delegate all other attributes/methods to original element"""
        return getattr(self._wrapped, name)

    def _generate_wind_mouse_path(self, start_x, start_y, dest_x, dest_y, gravity, wind, max_velocity, transition_dist):
        """
        Generate mouse trajectory using WindMouse algorithm
        Returns list of (x, y) coordinates
        """

        sqrt3 = np.sqrt(3)
        sqrt5 = np.sqrt(5)
        
        points = []
        start_x_f = float(start_x)
        start_y_f = float(start_y)
        dest_x_f = float(dest_x)
        dest_y_f = float(dest_y)
        
        current_x = int(round(start_x_f))
        current_y = int(round(start_y_f))
        points.append((current_x, current_y))
        
        v_x = v_y = W_x = W_y = 0.0
        
        while True:
            dx = dest_x_f - start_x_f
            dy = dest_y_f - start_y_f
            dist = np.hypot(dx, dy)
            
            if dist < 1:
                break
            
            W_mag = min(wind, dist)
            
            if dist >= transition_dist:
                W_x = W_x / sqrt3 + (2 * np.random.random() - 1) * W_mag / sqrt5
                W_y = W_y / sqrt3 + (2 * np.random.random() - 1) * W_mag / sqrt5
            else:
                W_x /= sqrt3
                W_y /= sqrt3
                if max_velocity < 3:
                    max_velocity = np.random.random() * 3 + 3
                else:
                    max_velocity /= sqrt5
            
            v_x += W_x + (gravity * dx) / dist
            v_y += W_y + (gravity * dy) / dist
            
            v_mag = np.hypot(v_x, v_y)
            if v_mag > max_velocity:
                v_clip = max_velocity / 2 + np.random.random() * max_velocity / 2
                v_x = (v_x / v_mag) * v_clip
                v_y = (v_y / v_mag) * v_clip
            
            start_x_f += v_x
            start_y_f += v_y
            
            move_x = int(round(start_x_f))
            move_y = int(round(start_y_f))
            
            if move_x != current_x or move_y != current_y:
                current_x = move_x
                current_y = move_y
                points.append((current_x, current_y))
        
        final_x = int(round(start_x_f))
        final_y = int(round(start_y_f))
        if (final_x, final_y) != (current_x, current_y):
            points.append((final_x, final_y))
        
        return points

    async def human_move(self, spread=20, gravity=9, wind=3, 
                        max_velocity=15, transition_dist=12,
                        min_delay=0.01, max_delay=0.03):
        bounds = await self.bounds
        dest_x, dest_y = self._calculate_center(bounds)
        dest_x += np.random.uniform(-spread, spread)
        dest_y += np.random.uniform(-spread, spread)
    
        start_x, start_y = await self._get_viewport_center()

        path = self._generate_wind_mouse_path(
            start_x, start_y, dest_x, dest_y,
            gravity, wind, max_velocity, transition_dist
        )

        for x, y in path:
            await self._connection_handler.execute_command(
                HumanInputCommands.mouse_move(x, y)
            )
            await asyncio.sleep(np.random.uniform(min_delay, max_delay))


    async def _get_viewport_center(self):
        """Get current viewport center coordinates"""
        cmd = RuntimeCommands.evaluate_script(
            "({width: window.innerWidth, height: window.innerHeight})",
        )
        cmd['params']['returnByValue'] = True
        viewport = await self._wrapped._connection_handler.execute_command(cmd)
        values = viewport['result']['result']['value']
        return values['width']/2, values['height']/2

def human_element(element):
    return HumanWebElement(element)
