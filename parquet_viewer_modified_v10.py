import pandas as pd
import configparser
import os
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from tkinter import font as tkfont

class ParquetViewer:
    def __init__(self, root):
        """Initialize the Parquet Viewer application."""
        self.root = root
        self.root.title("Parquet Data Viewer")

        # Define main window size
        window_width = 900
        window_height = 600
        # Set initial size of the main window
        self.root.geometry(f"{window_width}x{window_height}")

        # Center the main window on the screen
        # We must do this *before* creating and positioning the welcome window
        self.root.update_idletasks()  # Ensure Tkinter processes pending geometry changes for self.root
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        
        # Get the actual width and height after update_idletasks
        # This should match window_width and window_height unless overridden by window manager early
        actual_main_width = self.root.winfo_width()
        actual_main_height = self.root.winfo_height()
        
        x_coordinate = (screen_width - actual_main_width) // 2
        y_coordinate = (screen_height - actual_main_height) // 2
        
        # Set the final geometry (size and position) for the main window
        self.root.geometry(f"{actual_main_width}x{actual_main_height}+{x_coordinate}+{y_coordinate}")

        self.root.minsize(800, 500)
        
        # Configure the grid layout
        self.root.grid_columnconfigure(0, weight=1)
        self.root.grid_rowconfigure(1, weight=1)
        
        # Current loaded file path
        self.current_file_path = None
        
        # DataFrames for data and tracking
        self.df = None
        
        # Set up styles
        self.setup_styles()
        
        # Create UI elements
        self.create_header_frame()
        self.create_table_frame()
        self.create_status_bar()
        
        # Show welcome message - self.root is now centered
        self.show_welcome_window()
        
        # Do not load data automatically - user will need to select a file
    
    def setup_styles(self):
        """Configure the UI styles."""
        default_font = tkfont.nametofont("TkDefaultFont")
        default_font.configure(size=10)
        self.root.option_add("*Font", default_font)
        
        style = ttk.Style()
        style.configure("Treeview", rowheight=25)
        style.configure("Treeview.Heading", font=(None, 11, "bold"))
        style.configure("Header.TLabel", font=(None, 16, "bold"))
        style.configure("Status.TLabel", padding=5)
    
    def create_header_frame(self):
        """Create the header frame with title and buttons."""
        header_frame = ttk.Frame(self.root, padding="10")
        header_frame.grid(row=0, column=0, sticky="ew")
        header_frame.grid_columnconfigure(1, weight=1)

        title_label = ttk.Label(header_frame, text="Parquet Data Viewer", style="Header.TLabel")
        title_label.grid(row=0, column=0, sticky="w", padx=5)
        
        self.file_info_var = tk.StringVar()
        file_info_label = ttk.Label(header_frame, textvariable=self.file_info_var)
        file_info_label.grid(row=0, column=1, sticky="w", padx=10)
        
        buttons_frame = ttk.Frame(header_frame)
        buttons_frame.grid(row=0, column=2, sticky="e")
        
        open_btn = ttk.Button(buttons_frame, text="Select File", command=self.select_parquet_file)
        open_btn.pack(side=tk.RIGHT, padx=5)
        
        self.refresh_btn = ttk.Button(buttons_frame, text="Refresh Data", command=self.refresh_data)
        self.refresh_btn.pack(side=tk.RIGHT, padx=5)
        
        # Add Download button
        self.download_btn = ttk.Button(buttons_frame, text="Download", command=self.download_as_csv)
        self.download_btn.pack(side=tk.RIGHT, padx=5)
        # Initially disable the download button until a file is loaded
        self.download_btn.configure(state="disabled")
        
        info_btn = ttk.Button(buttons_frame, text="File Info", command=self.show_file_info)
        info_btn.pack(side=tk.RIGHT, padx=5)
    
    def create_table_frame(self):
        """Create the frame containing the data table with scrollbars."""
        self.table_frame = ttk.Frame(self.root)
        self.table_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)
        
        self.table_frame.grid_columnconfigure(0, weight=1)
        self.table_frame.grid_rowconfigure(0, weight=1)
        
        self.tree = ttk.Treeview(self.table_frame, show="headings")
        
        vsb = ttk.Scrollbar(self.table_frame, orient="vertical", command=self.tree.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=vsb.set)
        
        hsb = ttk.Scrollbar(self.table_frame, orient="horizontal", command=self.tree.xview)
        hsb.grid(row=1, column=0, sticky="ew")
        self.tree.configure(xscrollcommand=hsb.set)
        
        self.tree.grid(row=0, column=0, sticky="nsew")
        
        # Bind click event to show record details
        self.tree.bind("<Double-1>", self.show_record_details)
        self.tree.bind("<Return>", self.show_record_details)
    
    def create_status_bar(self):
        """Create a status bar at the bottom of the window."""
        status_frame = ttk.Frame(self.root)
        status_frame.grid(row=2, column=0, sticky="ew")
        status_frame.grid_columnconfigure(0, weight=1)
        status_frame.grid_columnconfigure(1, weight=0)
        status_frame.grid_columnconfigure(2, weight=0)
        
        self.status_var = tk.StringVar()
        status_bar_left = ttk.Label(status_frame, textvariable=self.status_var, 
                                    style="Status.TLabel", relief=tk.SUNKEN, anchor=tk.W)
        status_bar_left.grid(row=0, column=0, sticky="ew")
        self.status_var.set("Ready")
        
        # Create a frame for the progress bar
        progress_frame = ttk.Frame(status_frame)
        progress_frame.grid(row=0, column=1, sticky="e", padx=(5, 0))
        
        # Add progress bar in the status bar (initially hidden)
        self.progress_bar = ttk.Progressbar(progress_frame, orient=tk.HORIZONTAL, 
                                          length=150, mode='indeterminate')
        self.progress_bar.pack(side=tk.LEFT, padx=5)
        progress_frame.grid_remove()  # Initially hidden
        self.progress_frame = progress_frame  # Store reference
        
        self.record_count_var = tk.StringVar()
        status_bar_right = ttk.Label(status_frame, textvariable=self.record_count_var,
                                     style="Status.TLabel", relief=tk.SUNKEN, anchor=tk.E, width=20)
        status_bar_right.grid(row=0, column=2, sticky="e")
        self.record_count_var.set("Records: 0")
    
    def show_welcome_window(self):
        """Display a welcome window with information about the application."""
        welcome_window = tk.Toplevel(self.root)
        welcome_window.title("Welcome to Parquet Data Viewer")
        
        # Define welcome window dimensions - INCREASED HEIGHT from 400 to 500
        welcome_width = 600
        welcome_height = 500  # Increased from 400 to ensure the Get Started button is visible
        welcome_window.geometry(f"{welcome_width}x{welcome_height}")
        
        welcome_window.transient(self.root)
        welcome_window.grab_set()
        
        # Ensure the main window's geometry is fully updated before getting its coordinates
        self.root.update_idletasks() 
        
        # Calculate position for the welcome window to be in the center of the main window
        # self.root.winfo_x/y/width/height() should now return the correct, centered values
        # because the main window was positioned in __init__ before this method was called.
        root_x = self.root.winfo_x()
        root_y = self.root.winfo_y()
        root_width = self.root.winfo_width()
        root_height = self.root.winfo_height()
        
        # Calculate top-left (x, y) for the welcome window
        x_offset = root_x + (root_width - welcome_width) // 2
        y_offset = root_y + (root_height - welcome_height) // 2
        
        welcome_window.geometry(f"+{x_offset}+{y_offset}") # Set position
        
        # Add content to the welcome window
        welcome_frame = ttk.Frame(welcome_window, padding="20")
        welcome_frame.pack(fill=tk.BOTH, expand=True)
        
        title_label = ttk.Label(welcome_frame, text="Welcome to Parquet Data Viewer", 
                                font=(None, 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        info_text = (
            "This application allows you to view and explore Parquet files quickly and easily.\n\n"
            "Key Features:\n"
            "• Open and view any Parquet file with a simple interface\n"
            "• View file metadata and statistics\n"
            "• Automatically adjust column widths for better readability\n"
            "• Save file paths for quick access later\n"
            "• Double-click on a row to view detailed information\n"
            "• Download parquet data as CSV files\n\n"
            "Getting Started:\n"
            "1. Click 'Select File' to open a Parquet file\n"
            "2. Use 'File Info' to view metadata about the loaded file\n"
            "3. Click 'Download' to save the data as a CSV file\n"
            "4. The record count is displayed in the bottom right corner\n\n"
            "For questions or feedback, please contact debal-prasad.majumder@dxc.com."
        )
        
        info_label = ttk.Label(welcome_frame, text=info_text, wraplength=550, justify=tk.LEFT)
        info_label.pack(fill=tk.BOTH, expand=True, pady=10)
        
        ok_button = ttk.Button(welcome_frame, text="Get Started", 
                             command=welcome_window.destroy, width=15)
        ok_button.pack(pady=20)
        
        # Ensure welcome window is drawn and then wait for it to close
        welcome_window.update_idletasks() # Important for the Toplevel itself
        welcome_window.focus_set()
        self.root.wait_window(welcome_window)
    
    def show_file_info(self):
        """Show information about the loaded parquet file."""
        try:
            if not self.current_file_path:
                messagebox.showinfo("No File Loaded", "Please select a parquet file first.")
                return
                
            parquet_file_path = self.current_file_path
            file_size = os.path.getsize(parquet_file_path) / (1024 * 1024)  # Size in MB
            
            file_info = (
                f"File Path: {parquet_file_path}\n"
                f"File Size: {file_size:.2f} MB\n"
                f"Last Modified: {pd.Timestamp(os.path.getmtime(parquet_file_path), unit='s')}"
            )
            
            try:
                config = configparser.ConfigParser()
                config.read('config.properties')
                verbose = config.get('OPTIONS', 'verbose').strip()
                file_info += f"\nVerbose Mode: {verbose}"
            except:
                pass
            
            messagebox.showinfo("Parquet File Information", file_info)
            
        except Exception as e:
            messagebox.showerror("Error", f"Could not get file information: {str(e)}")
    
    def read_parquet_from_config(self, config_file_path='config.properties', file_path=None):
        """
        Read a parquet file using pandas, with the file location specified in a properties file
        or directly provided as an argument.
        """
        if file_path is not None:
            parquet_file_path = file_path
            verbose = True # Assuming verbose if loaded directly for now
        else:
            raise ValueError("No file path provided. Please select a parquet file.")
        
        # Check for file existence
        if not os.path.exists(parquet_file_path):
            raise FileNotFoundError(f"Parquet file not found: {parquet_file_path}")
        
        # Get verbose setting from config if available
        try:
            config = configparser.ConfigParser()
            config.read('config.properties')
            verbose = config.getboolean('OPTIONS', 'verbose')
        except:
            verbose = True  # Default to verbose if config can't be read
            
        if verbose:
            print(f"Reading parquet file: {parquet_file_path}")
        
        df = pd.read_parquet(parquet_file_path)
        if verbose:
            print(f"Successfully read {len(df)} rows and {len(df.columns)} columns")
            
        self.current_file_path = parquet_file_path
        self.df = df  # Store the DataFrame for later use
        return df, parquet_file_path
    
    def select_parquet_file(self):
        """Open a file dialog to select a parquet file."""
        try:
            try:
                config = configparser.ConfigParser()
                config.read('config.properties')
                current_path = config.get('FILE_PATHS', 'parquet_file_path').strip()
                initial_dir = os.path.dirname(current_path)
            except:
                initial_dir = os.getcwd()
            
            file_path = filedialog.askopenfilename(
                title="Select Parquet File",
                initialdir=initial_dir,
                filetypes=[("Parquet files", "*.parquet"), ("All files", "*.*")]
            )
            
            if file_path:
                self.update_config_file(file_path)
                self.load_data(file_path)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to select file: {str(e)}")
    
    def update_config_file(self, parquet_file_path):
        """Update the config file with the new parquet file path."""
        try:
            config = configparser.ConfigParser()
            
            if os.path.exists('config.properties'):
                config.read('config.properties')
            
            if not config.has_section('FILE_PATHS'):
                config.add_section('FILE_PATHS')
            if not config.has_section('OPTIONS'):
                config.add_section('OPTIONS')
                
            config.set('FILE_PATHS', 'parquet_file_path', parquet_file_path)
            
            if not config.has_option('OPTIONS', 'verbose'):
                config.set('OPTIONS', 'verbose', 'true')
                
            with open('config.properties', 'w') as config_file:
                config.write(config_file)
            return True
        except Exception as e:
            messagebox.showerror("Error", f"Failed to update config file: {str(e)}")
            return False
    
    def refresh_data(self):
        """Refresh data from the current file."""
        if self.current_file_path:
            # Call load_data with the current file path to ensure it's properly refreshed
            self.load_data(self.current_file_path)
        else:
            messagebox.showinfo("No File Loaded", "Please select a parquet file first.")
            
    def load_data(self, direct_file_path=None):
        """
        Load data from the Parquet file and populate the table.
        Shows a progress bar during loading.
        """
        try:
            self.status_var.set("Loading data...")
            self.refresh_btn.configure(state="disabled")
            self.download_btn.configure(state="disabled")  # Also disable download button during loading
            
            # Show and start progress bar
            self.progress_frame.grid()  # Show the frame containing the progress bar
            self.progress_bar.start(10)
            self.root.update_idletasks()
            
            # Use the provided file path, or fall back to the current file path
            file_path_to_use = direct_file_path if direct_file_path else self.current_file_path
            
            if not file_path_to_use:
                raise ValueError("No file path provided. Please select a parquet file.")
            
            df, file_path = self.read_parquet_from_config(file_path=file_path_to_use)
            
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            self.tree["columns"] = list(df.columns)
            
            for col in df.columns:
                self.tree.heading(col, text=col)
                max_width = max(len(str(col)), df[col].astype(str).str.len().max() if len(df) > 0 else 10) 
                width = min(max(max_width * 10, 100), 300)
                self.tree.column(col, width=width, minwidth=50)
            
            # Update progress occasionally while inserting rows
            total_rows = len(df)
            batch_size = max(1, total_rows // 10)  # Update progress about 10 times
            
            for i, row in df.iterrows():
                values = [str(val) if pd.notna(val) else "" for val in row]
                self.tree.insert("", "end", values=values)
                
                # Update status and progress bar periodically
                if i % batch_size == 0:
                    progress_percent = (i / total_rows) * 100 if total_rows > 0 else 100
                    self.status_var.set(f"Loading data... ({progress_percent:.0f}%)")
                    self.root.update_idletasks()
            
            file_name = os.path.basename(file_path)
            self.file_info_var.set(f"File: {file_name}")
            self.status_var.set(f"Loaded: {file_name}")
            
            record_count = len(df)
            self.record_count_var.set(f"Records: {record_count}")
            self.root.title(f"Parquet Data Viewer - {file_name}")
            
            # Enable download button now that a file is loaded
            self.download_btn.configure(state="normal")
            
        except Exception as e:
            messagebox.showerror("Error", str(e))
            self.status_var.set(f"Error: {str(e)}")
            self.record_count_var.set("Records: 0")
        finally:
            # Stop and hide progress bar
            self.progress_bar.stop()
            self.progress_frame.grid_remove()  # Hide the frame containing the progress bar
            self.refresh_btn.configure(state="normal")
    
    def download_as_csv(self):
        """Download the current parquet data as a CSV file."""
        try:
            if not self.current_file_path or self.df is None:
                messagebox.showinfo("No Data Available", "Please select a parquet file first.")
                return
                
            # Get the directory and filename from the current parquet file
            parquet_dir = os.path.dirname(self.current_file_path)
            parquet_filename = os.path.basename(self.current_file_path)
            
            # Create CSV filename (same name, different extension)
            base_filename = os.path.splitext(parquet_filename)[0]
            csv_filename = f"{base_filename}.csv"
            csv_filepath = os.path.join(parquet_dir, csv_filename)
            
            # Check if the CSV file already exists
            if os.path.exists(csv_filepath):
                response = messagebox.askyesno(
                    "File Already Exists", 
                    f"The file '{csv_filename}' already exists in this location. Do you want to overwrite it?"
                )
                if not response:
                    self.status_var.set("CSV download cancelled.")
                    return
            
            # Show downloading status
            self.status_var.set("Downloading CSV...")
            self.download_btn.configure(state="disabled")
            self.refresh_btn.configure(state="disabled")
            
            # Show and start progress bar
            self.progress_frame.grid()
            self.progress_bar.start(10)
            self.root.update_idletasks()
            
            # Write to CSV
            # Using a separate function to allow for updating the UI during long operations
            self.write_df_to_csv(csv_filepath)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to download CSV: {str(e)}")
            self.status_var.set(f"Error: {str(e)}")
        finally:
            # Stop and hide progress bar
            self.progress_bar.stop()
            self.progress_frame.grid_remove()
            self.download_btn.configure(state="normal")
            self.refresh_btn.configure(state="normal")
    
    def write_df_to_csv(self, csv_filepath):
        """Write the DataFrame to a CSV file with progress updates."""
        try:
            # For large files, we might want to show progress
            total_rows = len(self.df)
            
            # Start writing to CSV
            self.df.to_csv(csv_filepath, index=False)
            
            # Update status after successful write
            csv_filename = os.path.basename(csv_filepath)
            self.status_var.set(f"CSV file '{csv_filename}' saved successfully.")
            
            # Show a success message
            messagebox.showinfo(
                "Download Complete", 
                f"The file '{csv_filename}' has been saved successfully.\n\nLocation: {csv_filepath}"
            )
            
        except Exception as e:
            raise Exception(f"Error writing CSV file: {str(e)}")
    
    def show_record_details(self, event=None):
        """Show details of the selected record in a popup window."""
        try:
            # Get the selected item from the treeview
            selection = self.tree.selection()
            if not selection:
                return  # No selection
            
            # Get the selected item's values
            item = self.tree.item(selection[0])
            values = item['values']
            
            if not values or not self.df is not None:
                return
                
            # Get column names
            columns = self.tree.cget("columns")
            
            # Create a popup window to display the record details
            detail_window = tk.Toplevel(self.root)
            detail_window.title("Record Details")
            detail_window.geometry("500x400")
            detail_window.transient(self.root)
            detail_window.grab_set()
            
            # Center the detail window
            self.root.update_idletasks()
            root_x = self.root.winfo_x()
            root_y = self.root.winfo_y()
            root_width = self.root.winfo_width()
            root_height = self.root.winfo_height()
            window_width = 500
            window_height = 400
            x_offset = root_x + (root_width - window_width) // 2
            y_offset = root_y + (root_height - window_height) // 2
            detail_window.geometry(f"+{x_offset}+{y_offset}")
            
            # Create a frame with padding
            detail_frame = ttk.Frame(detail_window, padding="20")
            detail_frame.pack(fill=tk.BOTH, expand=True)
            
            # Add a title
            title_label = ttk.Label(
                detail_frame, 
                text="Record Details", 
                font=(None, 14, "bold")
            )
            title_label.pack(pady=(0, 10))
            
            # Create a frame for the fields
            fields_frame = ttk.Frame(detail_frame)
            fields_frame.pack(fill=tk.BOTH, expand=True)
            
            # Create a canvas and scrollbar for scrolling if needed
            canvas = tk.Canvas(fields_frame)
            scrollbar = ttk.Scrollbar(fields_frame, orient="vertical", command=canvas.yview)
            scrollable_frame = ttk.Frame(canvas)
            
            scrollable_frame.bind(
                "<Configure>",
                lambda e: canvas.configure(
                    scrollregion=canvas.bbox("all")
                )
            )
            
            canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)
            
            # Pack the scrollbar and canvas
            scrollbar.pack(side="right", fill="y")
            canvas.pack(side="left", fill="both", expand=True)
            
            # Add field and value pairs to the scrollable frame
            row_data = []
            for i, col in enumerate(columns):
                if i < len(values):
                    field_frame = ttk.Frame(scrollable_frame)
                    field_frame.pack(fill="x", padx=5, pady=2)
                    
                    # Field name (left)
                    field_label = ttk.Label(
                        field_frame, 
                        text=f"{col}:", 
                        width=20, 
                        anchor="w",
                        font=(None, 10, "bold")
                    )
                    field_label.pack(side="left", padx=(0, 10))
                    
                    # Field value (right)
                    value_label = ttk.Label(
                        field_frame, 
                        text=values[i], 
                        wraplength=300,
                        anchor="w"
                    )
                    value_label.pack(side="left", fill="x", expand=True)
                    
                    row_data.append((col, values[i]))
            
            # Add a close button
            close_button = ttk.Button(
                detail_frame, 
                text="Close",
                command=detail_window.destroy, 
                width=15
            )
            close_button.pack(pady=10)
            
        except Exception as e:
            messagebox.showerror("Error", f"Could not display record details: {str(e)}")

def main():
    """Main function to run the application."""
    root = tk.Tk()
    app = ParquetViewer(root) # ParquetViewer.__init__ now handles main window centering
    
    # Set app icon if available
    try:
        # You can replace this with a path to your own icon file
        # root.iconbitmap("path/to/icon.ico")
        pass
    except:
        pass
        
    root.mainloop()

if __name__ == "__main__":
    main()
