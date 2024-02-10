import plotly.express as px
import plotly.figure_factory as ff
import matplotlib.pyplot as plt

X_TICK_SKIP = 4
DPI = 300

def make_matplotlib_barh(fname:str, title:str, x, y, 
                         x_label:str, y_label:str, x_ticks:list=None, 
                         dataset=None) -> None:
    """
    Plot a horizontal bar graph of two given list or list-like dtypes
    - Parameters:
        - fname: File path to save in
        - title: Title of the Graph
        - x: x values to plot (str/list)
        - y: y values to plot (str/list)
        - x_label: label of the x-axis
        - y_label: label of the y-axis
        - x_ticks: Optional, change representation
                    of values in x-axis
        - dataset: Optional, instead get each x, y
                    from dataset's columns
    """
    fig, ax = plt.subplots(figsize=(10,5))

    plt.suptitle(title, fontweight='bold', fontsize=15)
    plt.rcParams['font.family'] = 'serif'

    if dataset:
        y_axis = list(dataset.select(y).toPandas()[y])
        x_axis = list(dataset.select(x).toPandas()[x])
    else:
        y_axis = y
        x_axis = x

    # Set the font parameters and values of the plot
    _ = plt.barh(x_axis, y_axis)
    ax.set_xlabel(x_label, fontname='serif',
                  fontdict={'weight': 'bold',
                            'size': 15})
    ax.set_ylabel(y_label, fontname='serif',
                  fontdict={'weight': 'bold',
                            'size': 15})
    if x_ticks:
        tick_positions = range(0, len(x_ticks))
        tick_labels = [x_ticks[i] for i in tick_positions]
        ax.set_xticks(tick_positions, tick_labels)
    else:
        ax.set_yticks(range(len(x_axis)))
        ax.set_yticklabels(x_axis)

    # Label the lines correctly
    fig.savefig(fname, dpi=DPI, bbox_inches='tight')
    return


def make_plotly_barh(title:str, x, y, 
                         x_label:str, y_label:str, x_ticks:list=None, 
                         dataset=None) -> None:
    """
    Plot a horizontal bar graph of two given list or list-like dtypes,
    Used for notebooks for more interactive UI
    - Parameters:
        - title: Title of the Graph
        - x: x values to plot (str/list)
        - y: y values to plot (str/list)
        - x_label: label of the x-axis
        - y_label: label of the y-axis
        - x_ticks: Optional, change representation
                    of values in x-axis
        - dataset: Optional, instead get each x, y
                    from dataset's columns
    """
    if dataset:
        fig = px.bar(dataset, x=x, y=y,
                            title=title,
                            orientation='h')
    else:
        fig = px.bar(y=x, x=y, 
                     title=title,
                     orientation='h')

    # Update x-tick values if not None
    if x_ticks:
        fig.update_xaxes(tickvals=x_ticks, 
        ticktext=[str(t) for t in x_ticks])

    # Update layout of plotly graph
    fig.update_layout(xaxis_title=x_label,
                      yaxis_title=y_label,
                      title = dict(x=0.5),
                      font = dict(size=20, family='Droid Serif'),
                      margin=dict(l=60, r=60, t=60, b=60),
                      height=len(x)*30)
    fig.show()
    return


def make_matplotlib_line(fname:str, title:str, x, y, x_label:str,
                          y_label:str, x_ticks:list=None, 
                          dataset=None) -> None:
    """
    Plot a line graph of two given list or list-like dtypes
    - Parameters:
        - fname: File path to save in
        - title: Title of the Graph
        - x: x values to plot (str/list)
        - y: y values to plot (str/list)
        - x_label: label of the x-axis
        - y_label: label of the y-axis
        - x_ticks: Optional, change representation
                    of values in x-axis
        - dataset: Optional, instead get each x, y
                    from dataset's columns
    """
    fig, ax = plt.subplots(figsize=(10,5))
    
    plt.rcParams['font.family'] = 'serif'
    plt.suptitle(title, fontweight='bold', fontsize=15)

    if dataset:
        y_axis = list(dataset.select(y).toPandas()[y])
        x_axis = list(dataset.select(x).toPandas()[x])
    else:
        y_axis = y
        x_axis = x

    plt.plot(x_axis, y_axis)
    ax.set_xlabel(x_label, fontname='serif', 
                  fontdict={'weight': 'bold', 'size': 15})
    ax.set_ylabel(y_label, fontname='serif', 
                  fontdict={'weight': 'bold', 'size': 15})
    if x_ticks:
        if len(x_ticks) <= 10:
            tick_positions = range(0, len(x_ticks))
        else:
            tick_positions = range(0, len(x_ticks), X_TICK_SKIP)
        tick_labels = [x_ticks[i] for i in tick_positions]
        ax.set_xticks(tick_positions, tick_labels, rotation=45, ha="right")
    else:
        pass

    fig.savefig(fname, dpi=DPI, bbox_inches='tight')
    return


def make_plotly_line(title:str, x, y, 
                        x_label:str, y_label:str, 
                        x_ticks:list=None,
                        dataset=None) -> None:
    """
    Plot a line graph of two given list or list-like dtypes
    Used for notebooks for more interactive UI
    - Parameters:

        - title: Title of the Graph
        - x: x values to plot
        - y: y values to plot
        - x_label: label of the x-axis
        - y_label: label of the y-axis
        - x_ticks: Optional, change representation
                    of values in x-axis
        - dataset: Optional, instead get each x, y
                    from dataset's columns
    """
    if dataset:
        fig = px.line(dataset, x=x, y=y,
                            title=title)
    else:
        fig = px.line(x=x, y=y, title=title)

    # Update x-tick values if not None
    if x_ticks:
        fig.update_xaxes(tickvals=x_ticks, 
        ticktext=[str(t) for t in x_ticks])

    # Update layout of plotly graph
    fig.update_layout(xaxis_title=x_label,
                      yaxis_title=y_label,
                      title = dict(x=0.5),
                      font = dict(size=20, family='Serif'),
                      margin=dict(l=60, r=60, t=60, b=60))
    fig.show()
    return


# We have to make our own special plotting function because of the relationship between the x-ticks and the x-labels
def make_matplotlib_line_irregular(fname:str, title:str, x, y, 
                                   x_label:str, y_label:str, y_pred:list, 
                                   x_ticks:list=None,
                                   legend:list=None ,dataset=None) -> None:
    """
    Makes a line plot, works when x values are not a clean sequence of 1,2,3...
    - Parameters
        - fname: Path for file saving
        - title: Title of plot
        - x: x values, positions in the graph (str/list)
        - y: y values, positions in the graph (str/list)
        - x_label: Name of X axis
        - y_label: Name of Y axis
        - y_pred: List in case you want to plot predicted
                  Put None if you don't want one
        - x_ticks: Used if you want to change the name of the x values
        - legend: List indicating names you want to set for both plots
                Assumes 2 labels
        - dataset: Used if you are indexing from a dataset with cols x, y
    - Returns
    -   None, but shows and saves a figure
    """
    fig, ax = plt.subplots(figsize=(10,5))
    
    plt.rcParams['font.family'] = 'serif'
    plt.suptitle(title, fontweight='bold', fontsize=15)

    if dataset:
        y_axis = list(dataset.select(y).toPandas()[y])
        x_axis = list(dataset.select(x).toPandas()[x])
    else:
        y_axis = y
        x_axis = x
    
    # need this to set the legends
    if legend:
        ax.plot(x_axis, y_axis, label=legend[0])
    # Otherwise just plot normally
    else:
        ax.plot(x_axis, y_axis)

    ax.set_xlabel(x_label, fontname='serif',
                  fontdict={'size': 15})
    ax.set_ylabel(y_label, fontname='serif',
                  fontdict={'size': 15})
    if x_ticks != None:
        # This is the change, instead we set tick_positions
        # to x, labels don't have to be indexed since
        # both x and x_ticks are proper order already
        if len(x_ticks) <= 10:
            tick_positions = range(0, len(x_ticks))
        else:
            tick_positions = range(0, len(x_ticks), X_TICK_SKIP)
        tick_labels = [x_ticks[i] for i in tick_positions]
        tick_positions = [x[i] for i in tick_positions]
        ax.set_xticks(tick_positions, tick_labels, rotation=45, ha="right")
        ax.tick_params(axis='x', labelsize=10)
        ax.tick_params(axis='y', labelsize=10)
    else:
        # ax.set_xticks(x_label, rotation=45, ha="right")
        pass

    if y_pred:
        # Get latter half of indices
        if legend:
            for i in range(len(y_pred)):
                back_len = len(y_pred[i])
                x_pred = list(x[-back_len:])
                ax.plot(x_pred, y_pred[i], label=legend[i+1])
                ax.legend(loc='upper right') 
        else:
            ax.plot(x_pred, y_pred)

    plt.subplots_adjust(bottom=0.1)
    # Label the lines correctly
    x_label = x_label.replace(" ", "_")
    y_label = y_label.replace (" ", "_")
    fig.savefig(fname, dpi=DPI, bbox_inches='tight')
    return


def make_plotly_line_irregular(title:str, x, y, 
                                x_label:str, y_label:str, y_pred:list, 
                                x_ticks:list=None,
                                legend:list=None ,dataset=None) -> None:
    """
    Makes a line plot, works when x values are not a clean sequence of 1,2,3...
    - Parameters
        - title: Title of plot
        - x: x values, positions in the graph (str/list)
        - y: y values, positions in the graph (str/list)
        - x_label: Name of X axis
        - y_label: Name of Y axis
        - y_pred: List in case you want to plot predicted
                  Put None if you don't want one
        - x_ticks: Used if you want to change the name of the x values
        - legend: List indicating names you want to set for both plots;
        - dataset: Used if you are indexing from a dataset using cols x, y
    - Returns
    -   None, but shows and saves a figure
    """

    actual_labels = [f'wide_variable_{i}' for i in range(len(legend))]
    map_labels = {}
    for i in range(len(legend)):
        map_labels[actual_labels[i]] = f'Line {i+1}'

    if not dataset:
        x_vals = x
        y_vals = y
    else:
        x_vals = dataset[x]
        y_vals = dataset[y]


    if y_pred and legend:
        fig = px.line(dataset, x=x_vals, 
                      y=[y_vals]+y_pred,
                      title=title,
                      labels=map_labels)
        for i in range(len(map_labels)):
            fig.update_traces(selector=dict(name=list(map_labels.keys())[i]),
                              name = legend[i])
    else:
        fig = px.line(dataset, x=x_vals, y=y_vals,
                            title=title)
        
    # Update x-tick values if not None
    if x_ticks:
        tick_vals = []
        ticktext = []
        for i in range(0, len(x_vals), 6):
            tick_vals.append(x_vals[i])
            ticktext.append(x_ticks[i])
        fig.update_xaxes(tickvals=tick_vals, 
        ticktext=ticktext)

    # # Position x-axis below the plot
    fig.update_xaxes(tickangle=45)

    # Update layout of plotly graph
    fig.update_layout(xaxis_title=x_label,
                      yaxis_title=y_label,
                      title = dict(x=0.5),
                      font = dict(size=20, family='Serif'),
                      margin=dict(l=60, r=60, t=60, b=60))
    fig.show()
    return